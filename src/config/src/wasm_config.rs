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

fn default_wasm_cache_dir() -> String {
    crate::config::paths::get_wasm_cache_dir()
        .to_string_lossy()
        .to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_cache_size() -> u64 {
    100 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    #[serde(default = "default_wasm_cache_dir")]
    pub cache_dir: String,
    #[serde(default = "default_true")]
    pub enable_cache: bool,
    #[serde(default = "default_max_cache_size")]
    pub max_cache_size: u64,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            cache_dir: default_wasm_cache_dir(),
            enable_cache: true,
            max_cache_size: default_max_cache_size(),
        }
    }
}
