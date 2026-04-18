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
use arrow::compute::{max, min};
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::{info, warn};

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::state::OperatorStateStore;
use crate::sql::common::time_utils::print_time;
use crate::sql::common::{
    CheckpointBarrier, FsSchema, FsSchemaRef, Watermark, from_nanos, to_nanos,
};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use async_trait::async_trait;

// ============================================================================
// WindowFunctionOperator: LSM-Tree backed lazy-compute model
// ============================================================================

pub struct WindowFunctionOperator {
    input_schema: FsSchemaRef,
    input_schema_unkeyed: FsSchemaRef,
    window_exec_plan: Arc<dyn ExecutionPlan>,
    receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,

    // LSM-Tree state engine and lightweight timestamp index
    state_store: Option<Arc<OperatorStateStore>>,
    pending_timestamps: BTreeSet<u64>,
}

impl WindowFunctionOperator {
    // State key: 8-byte big-endian timestamp (nanos)
    fn build_state_key(ts_nanos: u64) -> Vec<u8> {
        ts_nanos.to_be_bytes().to_vec()
    }

    fn extract_timestamp(key: &[u8]) -> Option<u64> {
        if key.len() == 8 {
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(key);
            Some(u64::from_be_bytes(ts_bytes))
        } else {
            None
        }
    }

    fn filter_and_split_batches(
        &self,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<Vec<(RecordBatch, SystemTime)>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let timestamp_column = self.input_schema.timestamp_column(&batch);
        let min_timestamp = from_nanos(min(timestamp_column).unwrap() as u128);
        let max_timestamp = from_nanos(max(timestamp_column).unwrap() as u128);

        if let Some(wm) = watermark
            && max_timestamp < wm
        {
            warn!(
                "dropped late batch: max_ts {} < watermark {}",
                print_time(max_timestamp),
                print_time(wm)
            );
            return Ok(vec![]);
        }

        if min_timestamp == max_timestamp {
            return Ok(vec![(batch, max_timestamp)]);
        }

        let sorted_batch = self
            .input_schema_unkeyed
            .sort(batch, true)
            .map_err(|e| anyhow!("sort for window fn: {e}"))?;
        let filtered_batch = self
            .input_schema_unkeyed
            .filter_by_time(sorted_batch, watermark)
            .map_err(|e| anyhow!("filter_by_time: {e}"))?;
        if filtered_batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let filtered_timestamps = self.input_schema.timestamp_column(&filtered_batch);
        let ranges = self
            .input_schema_unkeyed
            .partition(&filtered_batch, true)
            .map_err(|e| anyhow!("partition by time: {e}"))?;

        let mut batches = Vec::with_capacity(ranges.len());
        for range in ranges {
            let slice = filtered_batch.slice(range.start, range.end - range.start);
            let ts = from_nanos(filtered_timestamps.value(range.start) as u128);
            batches.push((slice, ts));
        }
        Ok(batches)
    }
}

#[async_trait]
impl Operator for WindowFunctionOperator {
    fn name(&self) -> &str {
        "WindowFunction"
    }

    // Recovery: restore the lightweight timestamp index from LSM-Tree.
    // Data stays on disk until process_watermark triggers on-demand compute.
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<()> {
        let store = OperatorStateStore::new(
            ctx.pipeline_id,
            ctx.state_dir.clone(),
            ctx.io_manager.clone(),
        )
        .map_err(|e| anyhow!("Failed to init state store: {e}"))?;

        let safe_epoch = ctx.latest_safe_epoch();
        let active_keys = store
            .restore_metadata(safe_epoch)
            .await
            .map_err(|e| anyhow!("State recovery failed: {e}"))?;

        if !active_keys.is_empty() {
            info!(
                pipeline_id = ctx.pipeline_id,
                key_count = active_keys.len(),
                "Window Function Operator recovering active timestamps from LSM-Tree..."
            );

            for key in active_keys {
                if let Some(ts_nanos) = Self::extract_timestamp(&key) {
                    self.pending_timestamps.insert(ts_nanos);
                }
            }

            info!(
                pipeline_id = ctx.pipeline_id,
                "Window Function Operator successfully rebuilt in-memory indices."
            );
        }

        self.state_store = Some(store);
        Ok(())
    }

    // Write-ahead: persist data into LSM-Tree, defer computation to watermark
    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let current_watermark = ctx.current_watermark();
        let split_batches = self.filter_and_split_batches(batch, current_watermark)?;
        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        for (sub_batch, timestamp) in split_batches {
            let ts_nanos = to_nanos(timestamp) as u64;
            let key = Self::build_state_key(ts_nanos);

            store
                .put(key, sub_batch)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            self.pending_timestamps.insert(ts_nanos);
        }

        Ok(vec![])
    }

    // On-demand compute & GC: pull data from LSM-Tree, run DataFusion, tombstone
    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let Watermark::EventTime(current_time) = watermark else {
            return Ok(vec![]);
        };
        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");
        let current_nanos = to_nanos(current_time) as u64;

        let expired_ts: Vec<u64> = self
            .pending_timestamps
            .iter()
            .take_while(|&&ts| ts < current_nanos)
            .copied()
            .collect();

        let mut final_outputs = Vec::new();

        for ts in expired_ts {
            let key = Self::build_state_key(ts);

            let batches = store.get_batches(&key).await.map_err(|e| anyhow!("{e}"))?;

            if !batches.is_empty() {
                let (tx, rx) = unbounded_channel();
                *self.receiver_hook.write().unwrap() = Some(rx);

                self.window_exec_plan.reset()?;
                let mut stream = self
                    .window_exec_plan
                    .execute(0, SessionContext::new().task_ctx())?;

                for batch in batches {
                    tx.send(batch)
                        .map_err(|e| anyhow!("Failed to send batch to execution plan: {e}"))?;
                }
                drop(tx);

                while let Some(res) = stream.next().await {
                    final_outputs.push(StreamOutput::Forward(res?));
                }
            }

            store.remove_batches(key).map_err(|e| anyhow!("{e}"))?;
            self.pending_timestamps.remove(&ts);
        }

        Ok(final_outputs)
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        self.state_store
            .as_ref()
            .expect("State store not initialized")
            .snapshot_epoch(barrier.epoch as u64)
            .map_err(|e| anyhow!("Snapshot failed: {e}"))?;
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// ============================================================================

pub struct WindowFunctionConstructor;

impl WindowFunctionConstructor {
    pub fn with_config(
        &self,
        config: protocol::function_stream_graph::WindowFunctionOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<WindowFunctionOperator> {
        let input_schema = Arc::new(
            FsSchema::try_from(
                config
                    .input_schema
                    .ok_or_else(|| anyhow!("missing input schema"))?,
            )
            .map_err(|e| anyhow!("input schema: {e}"))?,
        );

        let input_schema_unkeyed = Arc::new(
            FsSchema::from_schema_unkeyed(input_schema.schema.clone())
                .map_err(|e| anyhow!("unkeyed schema: {e}"))?,
        );

        let receiver_hook = Arc::new(RwLock::new(None));
        let codec = StreamingExtensionCodec {
            context: StreamingDecodingContext::UnboundedBatchStream(receiver_hook.clone()),
        };

        let window_exec_node =
            PhysicalPlanNode::decode(&mut config.window_function_plan.as_slice())
                .map_err(|e| anyhow!("decode window_function_plan: {e}"))?;
        let window_exec_plan = window_exec_node
            .try_into_physical_plan(
                registry.as_ref(),
                &RuntimeEnvBuilder::new().build()?,
                &codec,
            )
            .map_err(|e| anyhow!("window physical plan: {e}"))?;

        Ok(WindowFunctionOperator {
            input_schema,
            input_schema_unkeyed,
            window_exec_plan,
            receiver_hook,
            state_store: None,
            pending_timestamps: BTreeSet::new(),
        })
    }
}
