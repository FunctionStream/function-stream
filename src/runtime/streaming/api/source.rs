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
    /// 无数据可读：必须由 Runner 调度退避，禁止在 `fetch_next` 内长时间阻塞。
    Idle,
    EndOfStream,
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// 核心拉取：无数据时必须返回 [`SourceEvent::Idle`]，严禁内部阻塞控制面。
    async fn fetch_next(&mut self, ctx: &mut TaskContext) -> anyhow::Result<SourceEvent>;

    /// 独立于 `fetch_next` 的水位线脉搏（例如解决 Idle 时仍要推进水印）。
    fn poll_watermark(&mut self) -> Option<Watermark> {
        None
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()>;

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }
}
