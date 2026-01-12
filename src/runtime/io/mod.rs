// IO module - Stream task input/output module
// - Read data from network or sources
// - Data deserialization
// - Checkpoint barrier handling
// - Multi-input stream selection (multi-input only)
// - Data output to network

mod availability;
mod data_input_status;
mod data_output;
mod input_processor;
mod input_selection;
mod key_selection;
mod multiple_input_processor;
mod task_input;

// Re-export stream_element package contents
pub use availability::*;
pub use data_input_status::*;
pub use data_output::*;
pub use input_processor::*;

// Re-export input module contents for backward compatibility
