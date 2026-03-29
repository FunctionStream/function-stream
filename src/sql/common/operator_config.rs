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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::formats::{BadData, Format, Framing};
use super::fs_schema::FsSchema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub messages_per_second: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataField {
    pub field_name: String,
    pub key: String,
    /// JSON-encoded Arrow DataType string, e.g. `"Utf8"`, `"Int64"`.
    #[serde(default)]
    pub data_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorConfig {
    pub connection: Value,
    pub table: Value,
    pub format: Option<Format>,
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub metadata_fields: Vec<MetadataField>,
    #[serde(default)]
    pub input_schema: Option<FsSchema>,
}
