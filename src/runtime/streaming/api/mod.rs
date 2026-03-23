//! 接口层：算子与源实现需遵循的 trait 与运行时上下文。

pub mod context;
pub mod operator;
pub mod source;

pub use context::TaskContext;
pub use operator::{ConstructedOperator, MessageOperator};
pub use source::{SourceEvent, SourceOffset, SourceOperator};
