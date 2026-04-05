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

#[path = "../generated/cli/function_stream.rs"]
pub mod cli;

#[path = "../generated/service/function_stream.rs"]
pub mod service;

pub use cli::function_stream_service_client;
pub use cli::function_stream_service_client::FunctionStreamServiceClient;
pub use service::function_stream_service_server::{
    FunctionStreamService, FunctionStreamServiceServer,
};

pub mod function_stream_graph {
    #![allow(clippy::all)]
    include!("../generated/api/function_stream.v1.rs");
}

pub const FS_API_FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("fs_api_descriptor");

pub mod storage {
    #![allow(clippy::all)]
    #![allow(warnings)]
    include!("../generated/storage/function_stream.storage.rs");
}
