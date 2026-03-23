//! 与外部系统对接的 Sink 实现（Kafka 等）。

pub mod kafka;

pub use kafka::{ConsistencyMode, KafkaSinkOperator};
