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
use arrow::compute::kernels::aggregate;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use bincode::{Decode, Encode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::debug;

use crate::core::StreamOutput;
use crate::core::api::context::TaskContext;
use crate::core::api::operator::{Collector, Operator};
use crate::factory::Registry;
use crate::sql::common::{CheckpointBarrier, FsSchema, Watermark, from_nanos, to_millis};
use async_trait::async_trait;
use protocol::function_stream_graph::ExpressionWatermarkConfig;

#[derive(Debug, Copy, Clone, Encode, Decode, PartialEq, Eq)]
pub struct WatermarkGeneratorState {
    pub last_watermark_emitted_at: SystemTime,
    pub max_watermark: SystemTime,
}

impl Default for WatermarkGeneratorState {
    fn default() -> Self {
        Self {
            last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
            max_watermark: SystemTime::UNIX_EPOCH,
        }
    }
}

pub struct WatermarkGeneratorOperator {
    interval: Duration,
    idle_time: Option<Duration>,
    expression: Arc<dyn PhysicalExpr>,
    timestamp_index: usize,
    state: WatermarkGeneratorState,
    last_event_wall: SystemTime,
    is_idle: bool,
}

impl WatermarkGeneratorOperator {
    pub fn new(
        interval: Duration,
        idle_time: Option<Duration>,
        expression: Arc<dyn PhysicalExpr>,
        timestamp_index: usize,
    ) -> Self {
        Self {
            interval,
            idle_time,
            expression,
            timestamp_index,
            state: WatermarkGeneratorState::default(),
            last_event_wall: SystemTime::now(),
            is_idle: false,
        }
    }

    fn extract_max_timestamp(&self, batch: &RecordBatch) -> Option<SystemTime> {
        max_timestamp_from_array(batch.column(self.timestamp_index).as_ref()).map(from_nanos)
    }

    fn evaluate_watermark(&self, batch: &RecordBatch) -> Result<SystemTime> {
        let watermark_array = self
            .expression
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        let max_nanos = max_timestamp_from_array(watermark_array.as_ref()).ok_or_else(|| {
            anyhow!(
                "watermark expression must return a timestamp array, got {:?}",
                watermark_array.data_type()
            )
        })?;

        Ok(from_nanos(max_nanos))
    }
}

/// Returns event time as nanoseconds since epoch (internal watermark representation).
fn max_timestamp_from_array(array: &dyn Array) -> Option<u128> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array.as_primitive::<TimestampNanosecondType>();
            aggregate::max(arr).map(|v| v as u128)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array.as_primitive::<TimestampMicrosecondType>();
            aggregate::max(arr).map(|v| (v as u128) * 1_000)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array.as_primitive::<TimestampMillisecondType>();
            aggregate::max(arr).map(|v| (v as u128) * 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array.as_primitive::<TimestampSecondType>();
            aggregate::max(arr).map(|v| (v as u128) * 1_000_000_000)
        }
        other => {
            debug!(?other, "skip max timestamp: not a timestamp array");
            None
        }
    }
}

#[async_trait]
impl Operator for WatermarkGeneratorOperator {
    fn name(&self) -> &str {
        "ExpressionWatermarkGenerator"
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        self.last_event_wall = SystemTime::now();
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        self.last_event_wall = SystemTime::now();

        collector
            .collect(StreamOutput::Forward(batch.clone()), ctx)
            .await?;

        let Some(max_batch_ts) = self.extract_max_timestamp(&batch) else {
            return Ok(());
        };

        let new_watermark = self.evaluate_watermark(&batch)?;

        self.state.max_watermark = self.state.max_watermark.max(new_watermark);

        let time_since_last_emit = max_batch_ts
            .duration_since(self.state.last_watermark_emitted_at)
            .unwrap_or(Duration::ZERO);

        if self.is_idle || time_since_last_emit > self.interval {
            debug!(
                "[{}] emitting expression watermark {}",
                ctx.subtask_index,
                to_millis(self.state.max_watermark)
            );

            collector
                .collect(
                    StreamOutput::Watermark(Watermark::EventTime(self.state.max_watermark)),
                    ctx,
                )
                .await?;

            self.state.last_watermark_emitted_at = max_batch_ts;
            self.is_idle = false;
        }

        Ok(())
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        Ok(())
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![StreamOutput::Watermark(Watermark::EventTime(
            from_nanos(u64::MAX as u128),
        ))])
    }
}

pub struct WatermarkGeneratorConstructor;

impl WatermarkGeneratorConstructor {
    pub fn with_config(
        &self,
        config: ExpressionWatermarkConfig,
        registry: Arc<Registry>,
    ) -> anyhow::Result<WatermarkGeneratorOperator> {
        let input_schema: FsSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("missing input schema"))?
            .try_into()
            .map_err(|e| anyhow!("input schema: {e}"))?;
        let timestamp_index = input_schema.timestamp_index;

        let expression_node = PhysicalExprNode::decode(&mut config.expression.as_slice())
            .map_err(|e| anyhow!("decode expression: {e}"))?;
        let expression = parse_physical_expr(
            &expression_node,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )
        .map_err(|e| anyhow!("parse physical expr: {e}"))?;

        let interval = Duration::from_micros(config.period_micros);
        let idle_time = config.idle_time_micros.map(Duration::from_micros);

        Ok(WatermarkGeneratorOperator::new(
            interval,
            idle_time,
            expression,
            timestamp_index,
        ))
    }
}
