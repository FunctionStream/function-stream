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
use std::path::PathBuf;

pub const DEFAULT_PYTHON_WASM_FILENAME: &str = "functionstream-python-runtime.wasm";
pub const DEFAULT_PYTHON_CWASM_FILENAME: &str = "functionstream-python-runtime.cwasm";

fn default_python_wasm_path() -> String {
    super::paths::get_python_wasm_path()
        .to_string_lossy()
        .to_string()
}

fn default_python_cache_dir() -> String {
    super::paths::get_python_cache_dir()
        .to_string_lossy()
        .to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonConfig {
    #[serde(default = "default_python_wasm_path")]
    pub wasm_path: String,

    #[serde(default = "default_python_cache_dir")]
    pub cache_dir: String,

    #[serde(default = "default_true")]
    pub enable_cache: bool,
}

impl Default for PythonConfig {
    fn default() -> Self {
        Self {
            wasm_path: default_python_wasm_path(),
            cache_dir: default_python_cache_dir(),
            enable_cache: true,
        }
    }
}

impl PythonConfig {
    pub fn wasm_path_buf(&self) -> PathBuf {
        super::paths::resolve_path(&self.wasm_path)
    }

    pub fn cache_dir_buf(&self) -> PathBuf {
        super::paths::resolve_path(&self.cache_dir)
    }

    pub fn cwasm_cache_path(&self) -> PathBuf {
        super::paths::resolve_path(&self.cache_dir).join(DEFAULT_PYTHON_CWASM_FILENAME)
    }
}
