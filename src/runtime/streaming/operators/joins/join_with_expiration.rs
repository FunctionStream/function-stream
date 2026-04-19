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
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::state::OperatorStateStore;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use async_trait::async_trait;
use protocol::function_stream_graph::JoinOperator;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoinSide {
    Left = 0,
    Right = 1,
}

// ============================================================================
// Persistent state buffer: composite key [Side(1B)] + [Timestamp(8B BE)]
// ============================================================================

struct PersistentStateBuffer {
    side: JoinSide,
    ttl: Duration,
    active_timestamps: BTreeSet<u64>,
}

impl PersistentStateBuffer {
    fn new(side: JoinSide, ttl: Duration) -> Self {
        Self {
            side,
            ttl,
            active_timestamps: BTreeSet::new(),
        }
    }

    fn build_key(side: JoinSide, ts_nanos: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(side as u8);
        key.extend_from_slice(&ts_nanos.to_be_bytes());
        key
    }

    fn extract_timestamp(key: &[u8]) -> Option<u64> {
        if key.len() == 9 {
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&key[1..]);
            Some(u64::from_be_bytes(ts_bytes))
        } else {
            None
        }
    }

    async fn insert(
        &mut self,
        batch: RecordBatch,
        time: SystemTime,
        store: &Arc<OperatorStateStore>,
    ) -> Result<()> {
        let ts_nanos = time.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.active_timestamps.insert(ts_nanos);
        let key = Self::build_key(self.side, ts_nanos);
        store.put(key, batch).await.map_err(|e| anyhow!("{e}"))
    }

    fn expire(&mut self, current_time: SystemTime, store: &Arc<OperatorStateStore>) -> Result<()> {
        let cutoff = current_time.checked_sub(self.ttl).unwrap_or(UNIX_EPOCH);
        let cutoff_nanos = cutoff.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;

        let expired_ts: Vec<u64> = self
            .active_timestamps
            .iter()
            .take_while(|&&ts| ts < cutoff_nanos)
            .copied()
            .collect();

        for ts in expired_ts {
            let key = Self::build_key(self.side, ts);
            store.remove_batches(key).map_err(|e| anyhow!("{e}"))?;
            self.active_timestamps.remove(&ts);
        }

        Ok(())
    }

    async fn get_all_batches(&self, store: &Arc<OperatorStateStore>) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        for &ts in &self.active_timestamps {
            let key = Self::build_key(self.side, ts);
            let batches = store.get_batches(&key).await.map_err(|e| anyhow!("{e}"))?;
            all_batches.extend(batches);
        }
        Ok(all_batches)
    }
}

// ============================================================================
// JoinWithExpirationOperator
// ============================================================================

pub struct JoinWithExpirationOperator {
    left_input_schema: FsSchema,
    right_input_schema: FsSchema,
    left_schema: FsSchema,
    right_schema: FsSchema,

    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_exec_plan: Arc<dyn ExecutionPlan>,

    left_state: PersistentStateBuffer,
    right_state: PersistentStateBuffer,
    state_store: Option<Arc<OperatorStateStore>>,
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
        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        self.left_state.expire(current_time, store)?;
        self.right_state.expire(current_time, store)?;

        match side {
            JoinSide::Left => {
                self.left_state
                    .insert(batch.clone(), current_time, store)
                    .await?
            }
            JoinSide::Right => {
                self.right_state
                    .insert(batch.clone(), current_time, store)
                    .await?
            }
        }

        let opposite_batches = match side {
            JoinSide::Left => self.right_state.get_all_batches(store).await?,
            JoinSide::Right => self.left_state.get_all_batches(store).await?,
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

    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        let pipeline_block = ctx
            .pipeline_state_memory_block
            .as_ref()
            .ok_or_else(|| anyhow!("missing pipeline state memory block"))?;
        let store = OperatorStateStore::new(
            ctx.pipeline_id,
            ctx.state_dir.clone(),
            ctx.io_manager.clone(),
            Arc::clone(pipeline_block),
            ctx.operator_state_memory_bytes,
        )
        .map_err(|e| anyhow!("Failed to init state store: {e}"))?;

        let safe_epoch = ctx.latest_safe_epoch();
        let active_keys = store
            .restore_metadata(safe_epoch)
            .await
            .map_err(|e| anyhow!("State recovery failed: {e}"))?;

        for key in active_keys {
            if let Some(ts) = PersistentStateBuffer::extract_timestamp(&key) {
                if key[0] == JoinSide::Left as u8 {
                    self.left_state.active_timestamps.insert(ts);
                } else if key[0] == JoinSide::Right as u8 {
                    self.right_state.active_timestamps.insert(ts);
                }
            }
        }

        info!(
            pipeline_id = ctx.pipeline_id,
            restored_left = self.left_state.active_timestamps.len(),
            restored_right = self.right_state.active_timestamps.len(),
            "Join Operator restored state from LSM-Tree."
        );

        self.state_store = Some(store);
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
        barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        store
            .prepare_checkpoint_epoch(barrier.epoch as u64)
            .map_err(|e| anyhow!("Snapshot failed: {e}"))?;

        info!(epoch = barrier.epoch, "Join Operator snapshotted state.");
        Ok(())
    }

    async fn commit_checkpoint(&mut self, epoch: u32, _ctx: &mut TaskContext) -> Result<()> {
        self.state_store
            .as_ref()
            .expect("State store not initialized")
            .commit_checkpoint_epoch(epoch as u64)
            .map_err(|e| anyhow!("Commit checkpoint failed: {e}"))?;
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// Constructor
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
            left_state: PersistentStateBuffer::new(JoinSide::Left, ttl),
            right_state: PersistentStateBuffer::new(JoinSide::Right, ttl),
            state_store: None,
        })
    }
}
