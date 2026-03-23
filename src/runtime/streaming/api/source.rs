//! 源算子：由 [`crate::runtime::streaming::execution::SourceRunner`] 驱动 `fetch_next`，不得在内部死循环阻塞控制面。

use crate::runtime::streaming::api::context::TaskContext;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use crate::sql::common::{CheckpointBarrier, Watermark};

/// Kafka 等外部源在 **无已存位点** 时的起始消费策略（与 `arroyo-connectors` 语义对齐）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceOffset {
    Earliest,
    Latest,
    #[default]
    Group,
}

#[derive(Debug)]
pub enum SourceEvent {
    Data(RecordBatch),
    Watermark(Watermark),
    Idle,
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn fetch_next(&mut self, ctx: &mut TaskContext) -> anyhow::Result<SourceEvent>;

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()>;

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }
}
