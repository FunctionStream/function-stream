//! 与外部系统对接的源实现（Kafka 等）。

pub mod kafka;

pub use kafka::{BatchDeserializer, KafkaSourceOperator, KafkaState};
