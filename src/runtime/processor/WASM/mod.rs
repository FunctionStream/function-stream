pub mod thread_pool;
pub mod wasm_task;
pub mod wasm_processor;
pub mod wasm_processor_trait;
pub mod wasm_host;

// Re-export commonly used types
pub use wasm_task::{WasmTask, TaskEnvironment, ExecutionState};
pub use wasm_processor_trait::WasmProcessor;
pub use wasm_processor::{WasmProcessorImpl, WasmProcessorError};
pub use wasm_host::{HostState, create_wasm_host};
