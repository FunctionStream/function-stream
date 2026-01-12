// IO module - Stream task input/output module
// - Read data from network or sources
// - Data deserialization
// - Checkpoint barrier handling
// - Multi-input stream selection (multi-input only)
// - Data output to network

mod data_input_status;
mod input_processor;
mod task_input;
mod data_output;
mod availability;
mod input_selection;
mod multiple_input_processor;
mod key_selection;

// Re-export stream_element package contents
pub use crate::runtime::buffer_and_event::stream_element::*;
pub use data_input_status::*;
pub use input_processor::*;
pub use task_input::*;
pub use data_output::*;
pub use availability::*;
pub use input_selection::*;
pub use multiple_input_processor::*;
pub use key_selection::*;

// Re-export input module contents for backward compatibility
pub use crate::runtime::input::*;

