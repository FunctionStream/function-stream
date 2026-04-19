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
use arrow::compute::{concat_batches, max, min, partition, sort_to_indices, take};
use arrow_array::{RecordBatch, TimestampNanosecondArray};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::UNIX_EPOCH;
use tracing::{info, warn};

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::state::OperatorStateStore;
use crate::sql::common::{CheckpointBarrier, FsSchema, FsSchemaRef, Watermark};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use async_trait::async_trait;
use protocol::function_stream_graph::JoinOperator;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoinSide {
    Left = 0,
    Right = 1,
}

// ============================================================================
// Lightweight state index: composite key [Side(1B)] + [Timestamp(8B BE)]
// ============================================================================

struct InstantStateIndex {
    side: JoinSide,
    active_timestamps: BTreeSet<u64>,
}

impl InstantStateIndex {
    fn new(side: JoinSide) -> Self {
        Self {
            side,
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
}

// ============================================================================
// InstantJoinOperator (persistent state refactor)
// ============================================================================

pub struct InstantJoinOperator {
    left_input_schema: FsSchemaRef,
    right_input_schema: FsSchemaRef,
    left_schema: FsSchemaRef,
    right_schema: FsSchemaRef,

    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_exec_plan: Arc<dyn ExecutionPlan>,

    left_state: InstantStateIndex,
    right_state: InstantStateIndex,
    state_store: Option<Arc<OperatorStateStore>>,
}

impl InstantJoinOperator {
    fn input_schema(&self, side: JoinSide) -> FsSchemaRef {
        match side {
            JoinSide::Left => self.left_input_schema.clone(),
            JoinSide::Right => self.right_input_schema.clone(),
        }
    }

    async fn compute_pair(
        &mut self,
        left: RecordBatch,
        right: RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        self.left_passer.write().unwrap().replace(left);
        self.right_passer.write().unwrap().replace(right);

        self.join_exec_plan.reset().map_err(|e| anyhow!("{e}"))?;

        let mut result_stream = self
            .join_exec_plan
            .execute(0, SessionContext::new().task_ctx())
            .map_err(|e| anyhow!("{e}"))?;

        let mut outputs = Vec::new();
        while let Some(batch) = result_stream.next().await {
            outputs.push(batch.map_err(|e| anyhow!("{e}"))?);
        }
        Ok(outputs)
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
        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        let time_column = batch
            .column(self.input_schema(side).timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("Missing timestamp column"))?;

        let min_timestamp = min(time_column).ok_or_else(|| anyhow!("empty timestamp column"))?;
        let max_timestamp = max(time_column).ok_or_else(|| anyhow!("empty timestamp column"))?;

        if let Some(watermark) = ctx.current_watermark() {
            let watermark_nanos = watermark.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
            if watermark_nanos > min_timestamp {
                warn!("Dropped late batch from {:?} before watermark", side);
                return Ok(());
            }
        }

        let unkeyed_batch = self.input_schema(side).unkeyed_batch(&batch)?;
        let state_index = match side {
            JoinSide::Left => &mut self.left_state,
            JoinSide::Right => &mut self.right_state,
        };

        if max_timestamp == min_timestamp {
            let ts_nanos = max_timestamp as u64;
            let key = InstantStateIndex::build_key(side, ts_nanos);
            store
                .put(key, unkeyed_batch)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            state_index.active_timestamps.insert(ts_nanos);
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
            .unwrap();

        let ranges = partition(std::slice::from_ref(&sorted_timestamps))
            .unwrap()
            .ranges();

        for range in ranges {
            let sub_batch = sorted_batch.slice(range.start, range.end - range.start);
            let ts_nanos = typed_timestamps.value(range.start) as u64;
            let key = InstantStateIndex::build_key(side, ts_nanos);
            store
                .put(key, sub_batch)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            state_index.active_timestamps.insert(ts_nanos);
        }

        Ok(())
    }
}

#[async_trait]
impl Operator for InstantJoinOperator {
    fn name(&self) -> &str {
        "InstantJoin"
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
            if let Some(ts) = InstantStateIndex::extract_timestamp(&key) {
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
            "Instant Join Operator recovered state."
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
        let store = self.state_store.clone().unwrap();
        let cutoff_nanos = current_time.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;

        let mut all_active_ts = BTreeSet::new();
        all_active_ts.extend(self.left_state.active_timestamps.iter());
        all_active_ts.extend(self.right_state.active_timestamps.iter());

        let expired_ts: Vec<u64> = all_active_ts
            .into_iter()
            .filter(|&ts| ts < cutoff_nanos)
            .collect();

        if expired_ts.is_empty() {
            return Ok(vec![]);
        }

        // Phase 1: Harvest — extract all expired timestamp data from LSM-Tree
        let mut pending_pairs: Vec<(u64, RecordBatch, RecordBatch)> =
            Vec::with_capacity(expired_ts.len());

        for &ts in &expired_ts {
            let left_key = InstantStateIndex::build_key(JoinSide::Left, ts);
            let right_key = InstantStateIndex::build_key(JoinSide::Right, ts);

            let left_batches = store
                .get_batches(&left_key)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let right_batches = store
                .get_batches(&right_key)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let left_input = if left_batches.is_empty() {
                RecordBatch::new_empty(self.left_schema.schema.clone())
            } else {
                concat_batches(&self.left_schema.schema, left_batches.iter())?
            };
            let right_input = if right_batches.is_empty() {
                RecordBatch::new_empty(self.right_schema.schema.clone())
            } else {
                concat_batches(&self.right_schema.schema, right_batches.iter())?
            };

            pending_pairs.push((ts, left_input, right_input));
        }

        // Phase 2: Compute — all data extracted, no store reference held
        let mut emit_outputs = Vec::new();

        for (_, left_input, right_input) in pending_pairs {
            if left_input.num_rows() == 0 && right_input.num_rows() == 0 {
                continue;
            }
            let results = self.compute_pair(left_input, right_input).await?;
            for batch in results {
                emit_outputs.push(StreamOutput::Forward(batch));
            }
        }

        // Phase 3: Cleanup — tombstone LSM-Tree entries and update in-memory index
        for ts in expired_ts {
            let left_key = InstantStateIndex::build_key(JoinSide::Left, ts);
            let right_key = InstantStateIndex::build_key(JoinSide::Right, ts);
            store.remove_batches(left_key).map_err(|e| anyhow!("{e}"))?;
            store
                .remove_batches(right_key)
                .map_err(|e| anyhow!("{e}"))?;
            self.left_state.active_timestamps.remove(&ts);
            self.right_state.active_timestamps.remove(&ts);
        }

        Ok(emit_outputs)
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        self.state_store
            .as_ref()
            .unwrap()
            .snapshot_epoch(barrier.epoch as u64)
            .map_err(|e| anyhow!("Snapshot failed: {e}"))?;
        Ok(())
    }
}

// ============================================================================
// Constructor
// ============================================================================

pub struct InstantJoinConstructor;

impl InstantJoinConstructor {
    pub fn with_config(
        &self,
        config: JoinOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<InstantJoinOperator> {
        let left_input_schema: Arc<FsSchema> = Arc::new(config.left_schema.unwrap().try_into()?);
        let right_input_schema: Arc<FsSchema> = Arc::new(config.right_schema.unwrap().try_into()?);

        let left_schema = Arc::new(left_input_schema.schema_without_keys()?);
        let right_schema = Arc::new(right_input_schema.schema_without_keys()?);

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

        Ok(InstantJoinOperator {
            left_input_schema,
            right_input_schema,
            left_schema,
            right_schema,
            left_passer,
            right_passer,
            join_exec_plan,
            left_state: InstantStateIndex::new(JoinSide::Left),
            right_state: InstantStateIndex::new(JoinSide::Right),
            state_store: None,
        })
    }
}
