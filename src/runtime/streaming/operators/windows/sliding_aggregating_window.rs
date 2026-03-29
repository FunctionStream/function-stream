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

//! 滑动窗口聚合：纯内存版。
//! 完全依赖内部的 TieredRecordBatchHolder 和 ActiveBin 在内存中进行计算，
//! 摆脱 TableManager 依赖，遇到 Barrier 自动透传。

use anyhow::{anyhow, bail, Result};
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{Array, PrimitiveArray, RecordBatch, types::TimestampNanosecondType};
use arrow_schema::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{
    physical_plan::{from_proto::parse_physical_expr, AsExecutionPlan},
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use futures::StreamExt;
use prost::Message;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use async_trait::async_trait;
use crate::runtime::streaming::factory::Registry;
use protocol::grpc::api::SlidingWindowAggregateOperator;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{from_nanos, to_nanos, CheckpointBarrier, FsSchema, Watermark};
use crate::sql::physical::{DecodingContext, FsPhysicalExtensionCodec};
// ============================================================================
// 纯内存状态：阶梯式时间面板 (Tiered panes)
// 这部分本身就是极佳的内存数据结构，原样保留！
// ============================================================================

#[derive(Default, Debug)]
struct RecordBatchPane {
    batches: Vec<RecordBatch>,
}

#[derive(Debug)]
struct RecordBatchTier {
    width: Duration,
    start_time: Option<SystemTime>,
    panes: VecDeque<RecordBatchPane>,
}

impl RecordBatchTier {
    fn new(width: Duration) -> Self {
        Self {
            width,
            start_time: None,
            panes: VecDeque::new(),
        }
    }

    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let nanos = to_nanos(timestamp) - (to_nanos(timestamp) % self.width.as_nanos());
        from_nanos(nanos)
    }

    fn insert(&mut self, batch: RecordBatch, timestamp: SystemTime) -> Result<()> {
        let bin_start = self.bin_start(timestamp);
        if self.start_time.is_none() {
            self.start_time = Some(bin_start);
            self.panes.push_back(RecordBatchPane {
                batches: vec![batch],
            });
            return Ok(());
        }

        let start_time = self.start_time.unwrap();
        let bin_index =
            (bin_start.duration_since(start_time)?.as_nanos() / self.width.as_nanos()) as usize;
        while self.panes.len() <= bin_index {
            self.panes.push_back(RecordBatchPane::default());
        }
        self.panes[bin_index].batches.push(batch);
        Ok(())
    }

    fn batches_for_timestamp(&self, bin_start: SystemTime) -> Result<Vec<RecordBatch>> {
        if self
            .start_time
            .map(|st| st > bin_start)
            .unwrap_or(true)
        {
            return Ok(vec![]);
        }
        let bin_index = (bin_start
            .duration_since(self.start_time.unwrap())?
            .as_nanos()
            / self.width.as_nanos()) as usize;
        if self.panes.len() <= bin_index {
            return Ok(vec![]);
        }
        Ok(self.panes[bin_index].batches.clone())
    }

    fn delete_before(&mut self, cutoff: SystemTime) -> Result<()> {
        let bin_start = self.bin_start(cutoff);
        if self
            .start_time
            .map(|st| st >= bin_start)
            .unwrap_or(true)
        {
            return Ok(());
        }
        let bin_index = (bin_start
            .duration_since(self.start_time.unwrap())
            .unwrap()
            .as_nanos()
            / self.width.as_nanos()) as usize;

        if bin_index >= self.panes.len() {
            self.panes.clear();
        } else {
            self.panes.drain(0..bin_index);
        }
        self.start_time = Some(bin_start);
        Ok(())
    }
}

#[derive(Debug)]
struct TieredRecordBatchHolder {
    tier_widths: Vec<Duration>,
    tiers: Vec<RecordBatchTier>,
}

impl TieredRecordBatchHolder {
    fn new(tier_widths: Vec<Duration>) -> Result<Self> {
        for i in 0..tier_widths.len().saturating_sub(1) {
            if !tier_widths[i + 1].as_nanos().is_multiple_of(tier_widths[i].as_nanos()) {
                bail!(
                    "tier width {} does not evenly divide next {}",
                    tier_widths[i].as_nanos(),
                    tier_widths[i + 1].as_nanos()
                );
            }
        }
        let tiers = tier_widths
            .iter()
            .map(|w| RecordBatchTier::new(*w))
            .collect();
        Ok(Self { tier_widths, tiers })
    }

    fn insert(&mut self, batch: RecordBatch, timestamp: SystemTime) -> Result<()> {
        for tier in self.tiers.iter_mut() {
            tier.insert(batch.clone(), timestamp)?;
        }
        Ok(())
    }

    fn batches_for_interval(
        &self,
        interval_start: SystemTime,
        interval_end: SystemTime,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        let mut current_tier = 0usize;
        let mut current_start = interval_start;

        while current_start < interval_end {
            let tier_end = current_start + self.tier_widths[current_tier];
            if tier_end > interval_end {
                current_tier = current_tier.saturating_sub(1);
                continue;
            }
            if current_tier < self.tier_widths.len() - 1 {
                let next_tier = &self.tiers[current_tier + 1];
                if next_tier.bin_start(current_start) == current_start
                    && current_start + next_tier.width <= interval_end
                {
                    current_tier += 1;
                    continue;
                }
            }
            batches.extend(self.tiers[current_tier].batches_for_timestamp(current_start)?);
            current_start += self.tier_widths[current_tier];
        }
        if current_start != interval_end {
            bail!(
                "interval end {:?} does not match current start {:?}",
                interval_end, current_start
            );
        }
        Ok(batches)
    }

    fn delete_before(&mut self, cutoff: SystemTime) -> Result<()> {
        for tier in self.tiers.iter_mut() {
            tier.delete_before(cutoff)?;
        }
        Ok(())
    }
}

// ============================================================================
// Per-bin partial aggregation (纯内存缓冲区)
// ============================================================================

struct ActiveBin {
    sender: Option<UnboundedSender<RecordBatch>>,
    result_stream: Option<SendableRecordBatchStream>,
    finished_batches: Vec<RecordBatch>,
}

impl Default for ActiveBin {
    fn default() -> Self {
        Self {
            sender: None,
            result_stream: None,
            finished_batches: Vec::new(),
        }
    }
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

// ============================================================================
// 算子主体
// ============================================================================

pub struct SlidingWindowOperator {
    slide: Duration,
    width: Duration,
    binning_function: Arc<dyn PhysicalExpr>,

    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: FsSchema,

    finish_execution_plan: Arc<dyn ExecutionPlan>,
    final_projection: Arc<dyn ExecutionPlan>,
    projection_input_schema: SchemaRef,

    receiver_hook: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,

    active_bins: BTreeMap<SystemTime, ActiveBin>,
    tiered_record_batches: TieredRecordBatchHolder,
}

impl SlidingWindowOperator {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.slide == Duration::ZERO {
            return timestamp;
        }
        let nanos = to_nanos(timestamp) - (to_nanos(timestamp) % self.slide.as_nanos());
        from_nanos(nanos)
    }

    fn add_bin_start_as_timestamp(
        batch: &RecordBatch,
        bin_start: SystemTime,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let bin_start_scalar = ScalarValue::TimestampNanosecond(Some(to_nanos(bin_start) as i64), None);
        let timestamp_array = bin_start_scalar.to_array_of_size(batch.num_rows())?;
        let mut columns = batch.columns().to_vec();
        columns.push(timestamp_array);
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn ensure_bin_running(
        slot: &mut ActiveBin,
        plan: Arc<dyn ExecutionPlan>,
        hook: &Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    ) -> Result<()> {
        if slot.sender.is_some() {
            return Ok(());
        }
        let preserved = std::mem::take(&mut slot.finished_batches);
        let mut started = ActiveBin::start_partial(plan, hook)?;
        started.finished_batches = preserved;
        *slot = started;
        Ok(())
    }
}

#[async_trait]
impl MessageOperator for SlidingWindowOperator {
    fn name(&self) -> &str {
        "SlidingWindow"
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

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

        let watermark = ctx.last_present_watermark();

        for range in partition_ranges {
            let bin_start = from_nanos(typed_bin.value(range.start) as u128);

            if let Some(wm) = watermark {
                if bin_start < self.bin_start(wm) {
                    continue;
                }
            }

            let bin_batch = sorted.slice(range.start, range.end - range.start);
            let slot = self.active_bins.entry(bin_start).or_default();

            Self::ensure_bin_running(
                slot,
                self.partial_aggregation_plan.clone(),
                &self.receiver_hook,
            )?;

            let sender = slot
                .sender
                .as_ref()
                .ok_or_else(|| anyhow!("partial bin sender missing after ensure"))?;
            sender
                .send(bin_batch)
                .map_err(|e| anyhow!("partial channel send: {e}"))?;
        }

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
        let watermark_bin = self.bin_start(current_time);

        let mut final_outputs = Vec::new();

        let mut expired_bins = Vec::new();
        for &k in self.active_bins.keys() {
            if k + self.slide <= watermark_bin {
                expired_bins.push(k);
            } else {
                break;
            }
        }

        for bin_start in expired_bins {
            let mut bin = self
                .active_bins
                .remove(&bin_start)
                .ok_or_else(|| anyhow!("missing active bin"))?;
            let bin_end = bin_start + self.slide;

            bin.close_and_drain().await?;
            for b in bin.finished_batches {
                self.tiered_record_batches.insert(b, bin_start)?;
            }

            let interval_start = bin_end - self.width;
            let interval_end = bin_end;

            let partials = self
                .tiered_record_batches
                .batches_for_interval(interval_start, interval_end)?;
            *self.final_batches_passer.write().unwrap() = partials;

            self.finish_execution_plan.reset()?;
            let mut final_exec = self
                .finish_execution_plan
                .execute(0, SessionContext::new().task_ctx())?;

            let mut aggregate_results = Vec::new();
            while let Some(batch) = final_exec.next().await {
                aggregate_results.push(Self::add_bin_start_as_timestamp(
                    &batch?,
                    interval_start,
                    self.projection_input_schema.clone(),
                )?);
            }

            *self.final_batches_passer.write().unwrap() = aggregate_results;
            self.final_projection.reset()?;
            let mut proj_exec = self
                .final_projection
                .execute(0, SessionContext::new().task_ctx())?;

            while let Some(batch) = proj_exec.next().await {
                final_outputs.push(StreamOutput::Forward(batch?));
            }

            self.tiered_record_batches
                .delete_before(bin_end + self.slide - self.width)?;
        }

        Ok(final_outputs)
    }

    async fn snapshot_state(&mut self, _barrier: CheckpointBarrier, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ============================================================================
// 构造器
// ============================================================================

pub struct SlidingAggregatingWindowConstructor;

impl SlidingAggregatingWindowConstructor {
    pub fn with_config(
        &self,
        config: SlidingWindowAggregateOperator,
        registry: Arc<Registry>,
    ) -> anyhow::Result<SlidingWindowOperator> {
        let width = Duration::from_micros(config.width_micros);
        let slide = Duration::from_micros(config.slide_micros);
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

        let codec = FsPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver_hook.clone()),
        };
        let final_codec = FsPhysicalExtensionCodec {
            context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        let partial_plan = PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?
            .try_into_physical_plan(
                registry.as_ref(),
                &RuntimeEnvBuilder::new().build()?,
                &codec,
            )?;

        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?
            .try_into_physical_plan(
                registry.as_ref(),
                &RuntimeEnvBuilder::new().build()?,
                &final_codec,
            )?;

        let final_proj = PhysicalPlanNode::decode(&mut config.final_projection.as_slice())?
            .try_into_physical_plan(
                registry.as_ref(),
                &RuntimeEnvBuilder::new().build()?,
                &final_codec,
            )?;

        let partial_schema: FsSchema = config
            .partial_schema
            .ok_or_else(|| anyhow!("missing partial schema"))?
            .try_into()?;

        Ok(SlidingWindowOperator {
            slide,
            width,
            binning_function,
            partial_aggregation_plan: partial_plan,
            partial_schema,
            finish_execution_plan: finish_plan,
            final_projection: final_proj.clone(),
            projection_input_schema: final_proj.children()[0].schema().clone(),
            receiver_hook,
            final_batches_passer,
            active_bins: BTreeMap::new(),
            tiered_record_batches: TieredRecordBatchHolder::new(vec![slide])?,
        })
    }
}

