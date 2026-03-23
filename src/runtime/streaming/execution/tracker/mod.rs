//! 协调层：屏障对齐与多路水位线追踪。

pub mod barrier_aligner;
pub mod watermark_tracker;

pub use barrier_aligner::{AlignmentStatus, BarrierAligner};
pub use watermark_tracker::WatermarkTracker;
