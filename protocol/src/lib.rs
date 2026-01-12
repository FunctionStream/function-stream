// Protocol Buffers protocol definitions for function stream
// This module exports the generated Protocol Buffers code

// CLI module - exports client code
#[path = "../generated/cli/function_stream.rs"]
pub mod cli;

// Service module - exports server code
#[path = "../generated/service/function_stream.rs"]
pub mod service;

// Re-export commonly used types from both modules
// Data structures are the same in both, so we can re-export from either
pub use cli::function_stream_service_client;

// Re-export client-specific types
pub use cli::function_stream_service_client::FunctionStreamServiceClient;

// Re-export server-specific types
pub use service::function_stream_service_server::{
    FunctionStreamService, FunctionStreamServiceServer,
};
