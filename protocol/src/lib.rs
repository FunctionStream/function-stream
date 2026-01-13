// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
