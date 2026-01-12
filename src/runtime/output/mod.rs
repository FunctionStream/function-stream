// Output module - Output module
//
// Provides output implementations, including:
// - Output sink interface
// - Output sink provider (creates output sinks from configuration)
// - Output protocols (Kafka, etc.)

mod output_sink;
mod output_sink_provider;
mod protocol;

pub use output_sink::OutputSink;
pub use output_sink_provider::OutputSinkProvider;
