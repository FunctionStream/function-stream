//! 内置算子。

pub mod grouping;
pub mod joins;
pub mod sink;
pub mod source;
pub mod watermark;
pub mod windows;

pub use grouping::{
    IncrementalAggregatingConstructor, IncrementalAggregatingFunc, Key, UpdatingCache,
};
pub use joins::{
    InstantJoinConstructor, InstantJoinOperator, JoinWithExpirationConstructor,
    JoinWithExpirationOperator, LookupJoinConstructor, LookupJoinOperator, LookupJoinType,
};
pub use sink::{ConsistencyMode, KafkaSinkOperator};
pub use source::{BatchDeserializer, KafkaSourceOperator, KafkaState};
pub use watermark::{WatermarkGeneratorConstructor, WatermarkGeneratorOperator, WatermarkGeneratorState};
pub use windows::{
    SessionAggregatingWindowConstructor, SessionWindowOperator,
    SlidingAggregatingWindowConstructor, SlidingWindowOperator,
    TumblingAggregateWindowConstructor, TumblingWindowOperator, WindowFunctionConstructor,
    WindowFunctionOperator,
};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};

/// 透传数据。
pub struct PassthroughOperator {
    name: String,
}

impl PassthroughOperator {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl MessageOperator for PassthroughOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![StreamOutput::Forward(batch)])
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
