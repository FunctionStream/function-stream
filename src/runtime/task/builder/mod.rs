// Builder module - Task builder module
//
// Provides different types of task builders:
// - TaskBuilder: Main builder that dispatches to corresponding builders based on configuration type
// - ProcessorBuilder: Processor type task builder
// - SourceBuilder: Source type task builder (future support)
// - SinkBuilder: Sink type task builder (future support)

mod task_builder;
mod processor;
mod source;
mod sink;

pub use task_builder::TaskBuilder;
pub use processor::ProcessorBuilder;
pub use source::SourceBuilder;
pub use sink::SinkBuilder;

