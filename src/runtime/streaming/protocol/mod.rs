//! 协议层：数据事件、控制命令、水位线合并与比较语义。

pub mod control;
pub mod event;
pub mod stream_out;
pub mod tracked;
pub mod watermark;

pub use control::{
    control_channel, CheckpointBarrierWire, ControlCommand, StopMode,
};
pub use event::StreamEvent;
pub use stream_out::StreamOutput;
pub use tracked::TrackedEvent;
pub use watermark::{merge_watermarks, watermark_strictly_advances};
