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

use anyhow::{Result, anyhow};
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tracing::warn;

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use async_trait::async_trait;
use protocol::grpc::api::JoinOperator;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoinSide {
    Left,
    Right,
}

// ============================================================================
// ============================================================================

struct StateBuffer {
    batches: VecDeque<(SystemTime, RecordBatch)>,
    ttl: Duration,
}

impl StateBuffer {
    fn new(ttl: Duration) -> Self {
        Self {
            batches: VecDeque::new(),
            ttl,
        }
    }

    fn insert(&mut self, batch: RecordBatch, time: SystemTime) {
        self.batches.push_back((time, batch));
    }

    fn expire(&mut self, current_time: SystemTime) {
        let cutoff = current_time
            .checked_sub(self.ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        while let Some((time, _)) = self.batches.front() {
            if *time < cutoff {
                self.batches.pop_front();
            } else {
                break;
            }
        }
    }

    fn get_all_batches(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|(_, b)| b.clone()).collect()
    }
}

// ============================================================================
// ============================================================================

pub struct JoinWithExpirationOperator {
    left_input_schema: FsSchema,
    right_input_schema: FsSchema,
    left_schema: FsSchema,
    right_schema: FsSchema,

    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_exec_plan: Arc<dyn ExecutionPlan>,

    left_state: StateBuffer,
    right_state: StateBuffer,
}

impl JoinWithExpirationOperator {
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
        let current_time = ctx.current_watermark().unwrap_or_else(SystemTime::now);

        self.left_state.expire(current_time);
        self.right_state.expire(current_time);

        match side {
            JoinSide::Left => self.left_state.insert(batch.clone(), current_time),
            JoinSide::Right => self.right_state.insert(batch.clone(), current_time),
        }

        let opposite_batches = match side {
            JoinSide::Left => self.right_state.get_all_batches(),
            JoinSide::Right => self.left_state.get_all_batches(),
        };

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
impl Operator for JoinWithExpirationOperator {
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
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// ============================================================================

pub struct JoinWithExpirationConstructor;

impl JoinWithExpirationConstructor {
    pub fn with_config(
        &self,
        config: JoinOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<JoinWithExpirationOperator> {
        let left_passer = Arc::new(RwLock::new(None));
        let right_passer = Arc::new(RwLock::new(None));

        let codec = StreamingExtensionCodec {
            context: StreamingDecodingContext::LockedJoinPair {
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
            left_input_schema,
            right_input_schema,
            left_schema,
            right_schema,
            left_passer,
            right_passer,
            join_exec_plan,
            left_state: StateBuffer::new(ttl),
            right_state: StateBuffer::new(ttl),
        })
    }
}
