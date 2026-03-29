// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! 瞬时 JOIN：双通道喂入 DataFusion 物理计划，水位线推进时闭合实例并抽干结果（纯内存版）。

use anyhow::{anyhow, Result};
use arrow::compute::{max, min, partition, sort_to_indices, take};
use arrow_array::{RecordBatch, TimestampNanosecondArray};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::warn;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::factory::Registry;
use async_trait::async_trait;
use protocol::grpc::api::JoinOperator;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::constants::mem_exec_join_side;
use crate::sql::common::{from_nanos, CheckpointBarrier, FsSchema, FsSchemaRef, Watermark};
use crate::sql::physical::{DecodingContext, FsPhysicalExtensionCodec};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    #[allow(dead_code)]
    fn name(&self) -> &'static str {
        match self {
            JoinSide::Left => mem_exec_join_side::LEFT,
            JoinSide::Right => mem_exec_join_side::RIGHT,
        }
    }
}

/// 瞬时 JOIN 执行实例：保存通道；窗口闭合时关闭通道并同步抽干 `SendableRecordBatchStream`。
struct JoinInstance {
    left_tx: UnboundedSender<RecordBatch>,
    right_tx: UnboundedSender<RecordBatch>,
    result_stream: SendableRecordBatchStream,
}

impl JoinInstance {
    fn feed_data(&self, batch: RecordBatch, side: JoinSide) -> Result<()> {
        match side {
            JoinSide::Left => self
                .left_tx
                .send(batch)
                .map_err(|e| anyhow!("Left send err: {}", e)),
            JoinSide::Right => self
                .right_tx
                .send(batch)
                .map_err(|e| anyhow!("Right send err: {}", e)),
        }
    }

    /// 关闭输入流，促使执行计划结束，并拉取全部 JOIN 结果。
    async fn close_and_drain(self) -> Result<Vec<RecordBatch>> {
        drop(self.left_tx);
        drop(self.right_tx);

        let mut outputs = Vec::new();
        let mut stream = self.result_stream;

        while let Some(result_batch) = stream.next().await {
            outputs.push(result_batch?);
        }

        Ok(outputs)
    }
}

pub struct InstantJoinOperator {
    left_input_schema: FsSchemaRef,
    right_input_schema: FsSchemaRef,
    active_joins: BTreeMap<SystemTime, JoinInstance>,
    left_receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    right_receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    join_exec_plan: Arc<dyn ExecutionPlan>,
}

impl InstantJoinOperator {
    fn input_schema(&self, side: JoinSide) -> FsSchemaRef {
        match side {
            JoinSide::Left => self.left_input_schema.clone(),
            JoinSide::Right => self.right_input_schema.clone(),
        }
    }

    fn get_or_create_join_instance(&mut self, time: SystemTime) -> Result<&mut JoinInstance> {
        use std::collections::btree_map::Entry;

        if let Entry::Vacant(e) = self.active_joins.entry(time) {
            let (left_tx, left_rx) = unbounded_channel();
            let (right_tx, right_rx) = unbounded_channel();

            *self.left_receiver_hook.write().unwrap() = Some(left_rx);
            *self.right_receiver_hook.write().unwrap() = Some(right_rx);

            self.join_exec_plan.reset().map_err(|e| anyhow!("{e}"))?;
            let result_stream = self
                .join_exec_plan
                .execute(0, SessionContext::new().task_ctx())
                .map_err(|e| anyhow!("{e}"))?;

            e.insert(JoinInstance {
                left_tx,
                right_tx,
                result_stream,
            });
        }

        self.active_joins
            .get_mut(&time)
            .ok_or_else(|| anyhow!("join instance missing after insert"))
    }

    async fn process_side_internal(
        &mut self,
        side: JoinSide,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let time_column = batch
            .column(self.input_schema(side).timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("Missing timestamp column"))?;

        let min_timestamp = min(time_column).ok_or_else(|| anyhow!("empty timestamp column"))?;
        let max_timestamp = max(time_column).ok_or_else(|| anyhow!("empty timestamp column"))?;

        if let Some(watermark) = ctx.last_present_watermark() {
            if watermark > from_nanos(min_timestamp as u128) {
                warn!("Dropped late batch from {:?} before watermark", side);
                return Ok(());
            }
        }

        let unkeyed_batch = self.input_schema(side).unkeyed_batch(&batch)?;

        if max_timestamp == min_timestamp {
            let time_key = from_nanos(max_timestamp as u128);
            let join_instance = self.get_or_create_join_instance(time_key)?;
            join_instance.feed_data(unkeyed_batch, side)?;
            return Ok(());
        }

        let indices = sort_to_indices(time_column, None, None)?;
        let columns: Vec<_> = unkeyed_batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted_batch = RecordBatch::try_new(unkeyed_batch.schema(), columns)?;
        let sorted_timestamps = take(time_column, &indices, None).unwrap();
        let typed_timestamps = sorted_timestamps
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("sorted timestamps downcast failed"))?;
        let ranges = partition(std::slice::from_ref(&sorted_timestamps))
            .unwrap()
            .ranges();

        for range in ranges {
            let sub_batch = sorted_batch.slice(range.start, range.end - range.start);
            let time_key = from_nanos(typed_timestamps.value(range.start) as u128);
            let join_instance = self.get_or_create_join_instance(time_key)?;
            join_instance.feed_data(sub_batch, side)?;
        }

        Ok(())
    }
}

#[async_trait]
impl MessageOperator for InstantJoinOperator {
    fn name(&self) -> &str {
        "InstantJoin"
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
        self.process_side_internal(side, batch, ctx).await?;
        Ok(vec![])
    }

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let Watermark::EventTime(current_time) = watermark else {
            return Ok(vec![]);
        };
        let mut emit_outputs = Vec::new();

        let mut expired_times = Vec::new();
        for key in self.active_joins.keys() {
            if *key < current_time {
                expired_times.push(*key);
            } else {
                break;
            }
        }

        for time_key in expired_times {
            if let Some(join_instance) = self.active_joins.remove(&time_key) {
                let joined_batches = join_instance.close_and_drain().await?;
                for batch in joined_batches {
                    emit_outputs.push(StreamOutput::Forward(batch));
                }
            }
        }

        Ok(emit_outputs)
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        Ok(())
    }
}

/// 与 `OperatorConstructor` 类似的配置入口；返回 [`InstantJoinOperator`]（实现 [`MessageOperator`]），
/// 而非 `ConstructedOperator`（后者仅包装 `ArrowOperator`）。
pub struct InstantJoinConstructor;

impl InstantJoinConstructor {
    pub fn with_config(
        &self,
        config: JoinOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<InstantJoinOperator> {
        let join_physical_plan_node = PhysicalPlanNode::decode(&mut config.join_plan.as_slice())?;

        let left_input_schema: Arc<FsSchema> =
            Arc::new(config.left_schema.unwrap().try_into()?);
        let right_input_schema: Arc<FsSchema> =
            Arc::new(config.right_schema.unwrap().try_into()?);

        let left_receiver_hook = Arc::new(RwLock::new(None));
        let right_receiver_hook = Arc::new(RwLock::new(None));

        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::LockedJoinStream {
                left: left_receiver_hook.clone(),
                right: right_receiver_hook.clone(),
            },
        };

        let join_exec_plan = join_physical_plan_node.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        Ok(InstantJoinOperator {
            left_input_schema,
            right_input_schema,
            active_joins: BTreeMap::new(),
            left_receiver_hook,
            right_receiver_hook,
            join_exec_plan,
        })
    }
}
