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

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Udf {
    pub definition: String,
    #[serde(default)]
    pub language: UdfLanguage,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ValidateUdfPost {
    pub definition: String,
    #[serde(default)]
    pub language: UdfLanguage,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct UdfValidationResult {
    pub udf_name: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UdfLanguage {
    Python,
    #[default]
    Rust,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct UdfPost {
    pub prefix: String,
    #[serde(default)]
    pub language: UdfLanguage,
    pub definition: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct GlobalUdf {
    pub id: String,
    pub prefix: String,
    pub name: String,
    pub language: UdfLanguage,
    pub created_at: u64,
    pub updated_at: u64,
    pub definition: String,
    pub description: Option<String>,
    pub dylib_url: Option<String>,
}
