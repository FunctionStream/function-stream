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
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{Array, PrimitiveArray, RecordBatch, types::TimestampNanosecondType};
use arrow_schema::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, from_proto::parse_physical_expr},
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use futures::StreamExt;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{info, warn};

use crate::runtime::streaming::StreamOutput;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::factory::Registry;
use crate::runtime::streaming::state::OperatorStateStore;
use crate::sql::common::time_utils::print_time;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark, from_nanos, to_nanos};
use crate::sql::physical::{StreamingDecodingContext, StreamingExtensionCodec};
use crate::sql::schema::utils::add_timestamp_field_arrow;
use async_trait::async_trait;
use protocol::function_stream_graph::TumblingWindowAggregateOperator;

#[derive(Default)]
struct ActiveBin {
    sender: Option<UnboundedSender<RecordBatch>>,
    result_stream: Option<SendableRecordBatchStream>,
    finished_batches: Vec<RecordBatch>,
}

impl ActiveBin {
    fn start_partial(
        plan: Arc<dyn ExecutionPlan>,
        hook: &Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        *hook.write().unwrap() = Some(rx);
        plan.reset()?;
        let result_stream = plan.execute(0, SessionContext::new().task_ctx())?;
        Ok(Self {
            sender: Some(tx),
            result_stream: Some(result_stream),
            finished_batches: Vec::new(),
        })
    }

    async fn close_and_drain(&mut self) -> Result<()> {
        self.sender.take();
        if let Some(mut stream) = self.result_stream.take() {
            while let Some(batch) = stream.next().await {
                self.finished_batches.push(batch?);
            }
        }
        Ok(())
    }
}

pub struct TumblingWindowOperator {
    width: Duration,
    binning_function: Arc<dyn PhysicalExpr>,

    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: FsSchema,

    finish_execution_plan: Arc<dyn ExecutionPlan>,
    aggregate_with_timestamp_schema: SchemaRef,
    final_projection: Option<Arc<dyn ExecutionPlan>>,

    receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,

    active_bins: BTreeMap<SystemTime, ActiveBin>,

    // LSM-Tree state engine and pending window timestamp index
    state_store: Option<Arc<OperatorStateStore>>,
    pending_bins: BTreeSet<u64>,
}

impl TumblingWindowOperator {
    // State key: 8-byte big-endian bin_start_nanos
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

    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let nanos = to_nanos(timestamp) - (to_nanos(timestamp) % self.width.as_nanos());
        from_nanos(nanos)
    }

    fn add_bin_start_as_timestamp(
        batch: &RecordBatch,
        bin_start: SystemTime,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let bin_start_scalar =
            ScalarValue::TimestampNanosecond(Some(to_nanos(bin_start) as i64), None);
        let timestamp_array = bin_start_scalar.to_array_of_size(batch.num_rows())?;
        let mut columns = batch.columns().to_vec();
        columns.push(timestamp_array);
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| anyhow!("add _timestamp column: {e}"))
    }

    fn ensure_bin_running(
        slot: &mut ActiveBin,
        plan: Arc<dyn ExecutionPlan>,
        hook: &Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Result<()> {
        if slot.sender.is_some() {
            return Ok(());
        }
        let preserved = mem::take(&mut slot.finished_batches);
        let mut started = ActiveBin::start_partial(plan, hook)?;
        started.finished_batches = preserved;
        *slot = started;
        Ok(())
    }
}

#[async_trait]
impl Operator for TumblingWindowOperator {
    fn name(&self) -> &str {
        "TumblingWindow"
    }

    // Recovery: replay raw data from LSM-Tree into DataFusion sessions
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

        if !active_keys.is_empty() {
            info!(
                pipeline_id = ctx.pipeline_id,
                key_count = active_keys.len(),
                "Tumbling Window Operator recovering active windows from LSM-Tree..."
            );

            for key in active_keys {
                if let Some(ts_nanos) = Self::extract_timestamp(&key) {
                    let bin_start = from_nanos(ts_nanos as u128);

                    let batches = store.get_batches(&key).await.map_err(|e| anyhow!("{e}"))?;
                    if batches.is_empty() {
                        continue;
                    }

                    let slot = self.active_bins.entry(bin_start).or_default();
                    Self::ensure_bin_running(
                        slot,
                        self.partial_aggregation_plan.clone(),
                        &self.receiver_hook,
                    )?;

                    let sender = slot.sender.as_ref().unwrap();
                    for batch in batches {
                        sender
                            .send(batch)
                            .map_err(|e| anyhow!("recovery channel send: {e}"))?;
                    }

                    self.pending_bins.insert(ts_nanos);
                }
            }

            info!(
                pipeline_id = ctx.pipeline_id,
                "Tumbling Window Operator successfully replayed events and rebuilt in-memory state."
            );
        }

        self.state_store = Some(store);
        Ok(())
    }

    // Write-ahead: persist raw data before in-memory computation
    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let bin_array = self
            .binning_function
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let indices = sort_to_indices(bin_array.as_ref(), None, None)?;

        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns)?;
        let sorted_bins = take(bin_array.as_ref(), &indices, None)?;

        let typed_bin = sorted_bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .ok_or_else(|| anyhow!("binning function must produce TimestampNanosecond"))?;
        let partition_ranges = partition(std::slice::from_ref(&sorted_bins))?.ranges();

        let store = self
            .state_store
            .as_ref()
            .expect("State store not initialized");

        for range in partition_ranges {
            let bin_start = from_nanos(typed_bin.value(range.start) as u128);

            if let Some(watermark) = ctx.current_watermark()
                && bin_start < self.bin_start(watermark)
            {
                warn!(
                    "late data dropped: bin {} < watermark {}",
                    print_time(bin_start),
                    print_time(watermark)
                );
                continue;
            }

            let bin_batch = sorted.slice(range.start, range.end - range.start);
            let bin_start_nanos = to_nanos(bin_start) as u64;

            let state_key = Self::build_state_key(bin_start_nanos);
            store
                .put(state_key, bin_batch.clone())
                .await
                .map_err(|e| anyhow!("{e}"))?;
            self.pending_bins.insert(bin_start_nanos);

            let slot = self.active_bins.entry(bin_start).or_default();
            Self::ensure_bin_running(
                slot,
                self.partial_aggregation_plan.clone(),
                &self.receiver_hook,
            )?;

            let sender = slot
                .sender
                .as_ref()
                .ok_or_else(|| anyhow!("tumbling bin sender missing after ensure"))?;
            sender
                .send(bin_batch)
                .map_err(|e| anyhow!("partial channel send: {e}"))?;
        }

        Ok(vec![])
    }

    // Watermark-driven window closure with LSM-Tree GC
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

        let mut final_outputs = Vec::new();

        let mut expired_bins = Vec::new();
        for &k in self.active_bins.keys() {
            if k + self.width <= current_time {
                expired_bins.push(k);
            } else {
                break;
            }
        }

        for bin_start in expired_bins {
            let mut bin = self.active_bins.remove(&bin_start).unwrap();
            let bin_start_nanos = to_nanos(bin_start) as u64;

            bin.close_and_drain().await?;
            let partial_batches = mem::take(&mut bin.finished_batches);

            if partial_batches.is_empty() {
                continue;
            }

            *self.final_batches_passer.write().unwrap() = partial_batches;
            self.finish_execution_plan.reset()?;
            let mut final_exec = self
                .finish_execution_plan
                .execute(0, SessionContext::new().task_ctx())?;

            let mut aggregate_results = Vec::new();
            while let Some(batch) = final_exec.next().await {
                let batch = batch?;
                let with_timestamp = Self::add_bin_start_as_timestamp(
                    &batch,
                    bin_start,
                    self.aggregate_with_timestamp_schema.clone(),
                )?;

                if self.final_projection.is_none() {
                    final_outputs.push(StreamOutput::Forward(with_timestamp));
                } else {
                    aggregate_results.push(with_timestamp);
                }
            }

            if let Some(final_projection) = &self.final_projection {
                *self.final_batches_passer.write().unwrap() = aggregate_results;
                final_projection.reset()?;
                let mut proj_exec =
                    final_projection.execute(0, SessionContext::new().task_ctx())?;

                while let Some(batch) = proj_exec.next().await {
                    final_outputs.push(StreamOutput::Forward(batch?));
                }
            }

            // Tombstone the raw data — window is fully closed
            let state_key = Self::build_state_key(bin_start_nanos);
            store
                .remove_batches(state_key)
                .map_err(|e| anyhow!("{e}"))?;
            self.pending_bins.remove(&bin_start_nanos);
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

pub struct TumblingAggregateWindowConstructor;

impl TumblingAggregateWindowConstructor {
    pub fn with_config(
        &self,
        config: TumblingWindowAggregateOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<TumblingWindowOperator> {
        let width = Duration::from_micros(config.width_micros);
        let input_schema: FsSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("missing input schema"))?
            .try_into()?;

        let binning_function = parse_physical_expr(
            &PhysicalExprNode::decode(&mut config.binning_function.as_slice())?,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let receiver_hook = Arc::new(RwLock::new(None));
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));

        let codec = StreamingExtensionCodec {
            context: StreamingDecodingContext::UnboundedBatchStream(receiver_hook.clone()),
        };
        let final_codec = StreamingExtensionCodec {
            context: StreamingDecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        let partial_plan =
            PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?
                .try_into_physical_plan(
                    registry.as_ref(),
                    &RuntimeEnvBuilder::new().build()?,
                    &codec,
                )?;

        let partial_schema: FsSchema = config
            .partial_schema
            .ok_or_else(|| anyhow!("missing partial schema"))?
            .try_into()?;

        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;
        let finish_execution_plan = finish_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &final_codec,
        )?;

        let final_projection_plan = match &config.final_projection {
            Some(proto) if !proto.is_empty() => {
                let node = PhysicalPlanNode::decode(&mut proto.as_slice())
                    .map_err(|e| anyhow!("decode final_projection: {e}"))?;
                Some(node.try_into_physical_plan(
                    registry.as_ref(),
                    &RuntimeEnvBuilder::new().build()?,
                    &final_codec,
                )?)
            }
            _ => None,
        };

        let aggregate_with_timestamp_schema =
            add_timestamp_field_arrow((*finish_execution_plan.schema()).clone());

        Ok(TumblingWindowOperator {
            width,
            binning_function,
            partial_aggregation_plan: partial_plan,
            partial_schema,
            finish_execution_plan,
            aggregate_with_timestamp_schema,
            final_projection: final_projection_plan,
            receiver_hook,
            final_batches_passer,
            active_bins: BTreeMap::new(),
            state_store: None,
            pending_bins: BTreeSet::new(),
        })
    }
}
