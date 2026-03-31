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

//! REST/RPC API types for the FunctionStream system.
//!
//! Adapted from Arroyo's `arroyo-rpc/src/api_types` and utility modules.

pub mod checkpoints;
pub mod connections;
pub mod metrics;
pub mod pipelines;
pub mod public_ids;
pub mod schema_resolver;
pub mod udfs;
pub mod var_str;

use serde::{Deserialize, Serialize};

pub use connections::ConnectionProfile;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedCollection<T> {
    pub data: Vec<T>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NonPaginatedCollection<T> {
    pub data: Vec<T>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PaginationQueryParams {
    pub starting_after: Option<String>,
    pub limit: Option<u32>,
}
