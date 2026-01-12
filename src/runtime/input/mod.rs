// Input module - Input module
//
// Provides input implementations for various data sources, including:
// - Input source interface
// - Input source provider (creates input sources from configuration)
// - Input source protocols (Kafka, etc.)

mod input_source;
mod input_source_provider;
pub mod protocol;

pub use input_source::{InputSource, InputSourceState, CONTROL_TASK_CHANNEL_CAPACITY};
pub use input_source_provider::InputSourceProvider;

