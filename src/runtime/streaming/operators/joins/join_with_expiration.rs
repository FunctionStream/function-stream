//! 带 TTL 的 Key-Time Join：两侧状态表 + DataFusion 物理计划成对计算。

use anyhow::{anyhow, Result};
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::warn;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use async_trait::async_trait;
use tracing_subscriber::Registry;
use protocol::grpc::api::JoinOperator;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark};
use crate::sql::logical_planner::{DecodingContext, FsPhysicalExtensionCodec};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    fn table_name(&self) -> &'static str {
        match self {
            JoinSide::Left => "left",
            JoinSide::Right => "right",
        }
    }
}

pub struct JoinWithExpirationOperator {
    /// 保留与配置/表注册语义一致；实际 TTL 由状态表配置决定。
    #[allow(dead_code)]
    left_expiration: Duration,
    #[allow(dead_code)]
    right_expiration: Duration,
    left_input_schema: FsSchema,
    right_input_schema: FsSchema,
    left_schema: FsSchema,
    right_schema: FsSchema,
    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_exec_plan: Arc<dyn ExecutionPlan>,
}

impl JoinWithExpirationOperator {
    /// 执行 DataFusion 物理计划，返回 JOIN 结果批次（不经过 Collector）。
    async fn compute_pair(
        &mut self,
        left: RecordBatch,
        right: RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        if left.num_rows() == 0 || right.num_rows() == 0 {
            return Ok(vec![]);
        }

        {
            self.left_passer.write().unwrap().replace(left);
            self.right_passer.write().unwrap().replace(right);
        }

        self.join_exec_plan
            .reset()
            .map_err(|e| anyhow!("join plan reset: {e}"))?;
        let mut result_stream = self
            .join_exec_plan
            .execute(0, SessionContext::new().task_ctx())
            .map_err(|e| anyhow!("join execute: {e}"))?;

        let mut outputs = Vec::new();
        while let Some(batch) = result_stream.next().await {
            outputs.push(batch.map_err(|e| anyhow!("{e}"))?);
        }

        Ok(outputs)
    }

    async fn process_side(
        &mut self,
        side: JoinSide,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let watermark = ctx.last_present_watermark();
        let target_name = side.table_name();
        let opposite_name = match side {
            JoinSide::Left => JoinSide::Right.table_name(),
            JoinSide::Right => JoinSide::Left.table_name(),
        };

        let mut tm = ctx.table_manager_guard().await?;

        let inserted_rows = {
            let target_table = tm
                .get_key_time_table(target_name, watermark)
                .await
                .map_err(|e| anyhow!("{e:?}"))?;
            target_table
                .insert(batch.clone())
                .await
                .map_err(|e| anyhow!("{e:?}"))?
        };

        let opposite_table = tm
            .get_key_time_table(opposite_name, watermark)
            .await
            .map_err(|e| anyhow!("{e:?}"))?;

        let mut opposite_batches = Vec::new();
        for row in inserted_rows {
            if let Some(matched_batch) = opposite_table
                .get_batch(row.as_ref())
                .map_err(|e| anyhow!("{e:?}"))?
            {
                opposite_batches.push(matched_batch.clone());
            }
        }

        drop(tm);

        if opposite_batches.is_empty() {
            return Ok(vec![]);
        }

        let opposite_schema = match side {
            JoinSide::Left => &self.right_schema.schema,
            JoinSide::Right => &self.left_schema.schema,
        };
        let combined_opposite_batch = concat_batches(opposite_schema, opposite_batches.iter())?;

        let unkeyed_target_batch = match side {
            JoinSide::Left => self.left_input_schema.unkeyed_batch(&batch)?,
            JoinSide::Right => self.right_input_schema.unkeyed_batch(&batch)?,
        };

        let (left_input, right_input) = match side {
            JoinSide::Left => (unkeyed_target_batch, combined_opposite_batch),
            JoinSide::Right => (combined_opposite_batch, unkeyed_target_batch),
        };

        let result_batches = self.compute_pair(left_input, right_input).await?;

        Ok(result_batches
            .into_iter()
            .map(StreamOutput::Forward)
            .collect())
    }
}

#[async_trait]
impl MessageOperator for JoinWithExpirationOperator {
    fn name(&self) -> &str {
        "JoinWithExpiration"
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let side = if input_idx == 0 {
            JoinSide::Left
        } else {
            JoinSide::Right
        };
        self.process_side(side, batch, ctx).await
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        // `KeyTimeView` 无 `flush`；写入已通过 `insert` 经 `state_tx` 进入后端刷写管线，
        // 与 worker 侧 `JoinWithExpiration` 未单独实现 `handle_checkpoint` 一致。
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

/// 从配置构造 [`JoinWithExpirationOperator`]（实现 [`MessageOperator`]）。
/// 注意：`ConstructedOperator` 仅包装 `ArrowOperator`，此处不返回该类型。
pub struct JoinWithExpirationConstructor;

impl JoinWithExpirationConstructor {
    pub fn with_config(
        &self,
        config: JoinOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<JoinWithExpirationOperator> {
        let left_passer = Arc::new(RwLock::new(None));
        let right_passer = Arc::new(RwLock::new(None));

        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::LockedJoinPair {
                left: left_passer.clone(),
                right: right_passer.clone(),
            },
        };

        let join_physical_plan_node = PhysicalPlanNode::decode(&mut config.join_plan.as_slice())?;
        let join_exec_plan = join_physical_plan_node.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        let left_input_schema: FsSchema = config.left_schema.unwrap().try_into()?;
        let right_input_schema: FsSchema = config.right_schema.unwrap().try_into()?;
        let left_schema = left_input_schema.schema_without_keys()?;
        let right_schema = right_input_schema.schema_without_keys()?;

        let mut ttl = Duration::from_micros(
            config
                .ttl_micros
                .expect("ttl must be set for non-instant join"),
        );

        if ttl == Duration::ZERO {
            warn!("TTL was not set for join with expiration, defaulting to 24 hours.");
            ttl = Duration::from_secs(24 * 60 * 60);
        }

        Ok(JoinWithExpirationOperator {
            left_expiration: ttl,
            right_expiration: ttl,
            left_input_schema,
            right_input_schema,
            left_schema,
            right_schema,
            left_passer,
            right_passer,
            join_exec_plan,
        })
    }
}
