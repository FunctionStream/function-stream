use arrow_array::RecordBatch;
use crate::sql::common::{CheckpointBarrier, Watermark};

/// 核心数据面事件
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Data(RecordBatch),
    Watermark(Watermark),
    Barrier(CheckpointBarrier),
    EndOfStream,
}
