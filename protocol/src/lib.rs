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

// ─────────────── FunctionStream Service (original) ───────────────

#[path = "../generated/cli/function_stream.rs"]
pub mod cli;

#[path = "../generated/service/function_stream.rs"]
pub mod service;

pub use cli::function_stream_service_client;
pub use cli::function_stream_service_client::FunctionStreamServiceClient;
pub use service::function_stream_service_server::{
    FunctionStreamService, FunctionStreamServiceServer,
};

// ─────────────── Streaming Pipeline API (fs_api.proto) ───────────────

pub mod grpc {
    /// Serde-annotated API types for streaming operators, schemas, programs.
    #[allow(clippy::all)]
    pub mod api {
        include!("../generated/api/fs_api.rs");
    }
}

/// File descriptor set for fs_api.proto (for gRPC reflection / REST gateway).
pub const FS_API_FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("fs_api_descriptor");

// ─────────────── Durable storage (storage.proto: catalog + task rows) ───────────────

/// Prost types for persisted stream catalog and task storage (`proto/storage.proto`).
pub mod storage {
    #![allow(clippy::all)]
    #![allow(warnings)]
    include!("../generated/storage/function_stream.storage.rs");
}
