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


use anyhow::{anyhow, Result};
use arrow::compute::kernels::aggregate;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{RecordBatch, TimestampNanosecondArray};
use bincode::{Decode, Encode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, info};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::factory::Registry;
use async_trait::async_trait;
use protocol::grpc::api::ExpressionWatermarkConfig;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{from_nanos, to_millis, CheckpointBarrier, FsSchema, Watermark};

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
        let ts_column = batch.column(self.timestamp_index);
        let arr = ts_column.as_primitive::<TimestampNanosecondType>();
        let max_ts = aggregate::max(arr)?;
        Some(from_nanos(max_ts as u128))
    }

    fn evaluate_watermark(&self, batch: &RecordBatch) -> Result<SystemTime> {
        let watermark_array = self
            .expression
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        let typed_array = watermark_array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("watermark expression must return TimestampNanosecondArray"))?;

        let max_watermark_nanos = aggregate::max(typed_array)
            .ok_or_else(|| anyhow!("failed to extract max watermark from batch"))?;

        Ok(from_nanos(max_watermark_nanos as u128))
    }
}

#[async_trait]
impl MessageOperator for WatermarkGeneratorOperator {
    fn name(&self) -> &str {
        "ExpressionWatermarkGenerator"
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
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
    ) -> Result<Vec<StreamOutput>> {
        self.last_event_wall = SystemTime::now();

        let mut outputs = vec![StreamOutput::Forward(batch.clone())];

        let Some(max_batch_ts) = self.extract_max_timestamp(&batch) else {
            return Ok(outputs);
        };

        let new_watermark = self.evaluate_watermark(&batch)?;

        self.state.max_watermark = self.state.max_watermark.max(new_watermark);

        let time_since_last_emit = max_batch_ts
            .duration_since(self.state.last_watermark_emitted_at)
            .unwrap_or(Duration::ZERO);

        if self.is_idle || time_since_last_emit > self.interval {
            debug!(
                "[{}] emitting expression watermark {}",
                ctx.subtask_idx,
                to_millis(self.state.max_watermark)
            );

            outputs.push(StreamOutput::Watermark(Watermark::EventTime(
                self.state.max_watermark,
            )));

            self.state.last_watermark_emitted_at = max_batch_ts;
            self.is_idle = false;
        }

        Ok(outputs)
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn process_tick(
        &mut self,
        _tick_index: u64,
        ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        if let Some(idle_timeout) = self.idle_time {
            let elapsed = self
                .last_event_wall
                .elapsed()
                .unwrap_or(Duration::ZERO);
            if !self.is_idle && elapsed > idle_timeout {
                info!(
                    "task [{}] entering Idle after {:?}",
                    ctx.subtask_idx, idle_timeout
                );
                self.is_idle = true;
                return Ok(vec![StreamOutput::Watermark(Watermark::Idle)]);
            }
        }
        Ok(vec![])
    }

    async fn snapshot_state(&mut self, _barrier: CheckpointBarrier, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![StreamOutput::Watermark(Watermark::EventTime(from_nanos(
            u64::MAX as u128,
        )))])
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

        let expression_node =
            PhysicalExprNode::decode(&mut config.expression.as_slice()).map_err(|e| {
                anyhow!("decode expression: {e}")
            })?;
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

