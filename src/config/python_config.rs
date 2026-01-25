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

/// Default directory for Python runtime cache (wasm and cwasm files)
pub const DEFAULT_PYTHON_CACHE_DIR: &str = "data/cache/python-runner";

/// Default Python WASM filename
pub const DEFAULT_PYTHON_WASM_FILENAME: &str = "functionstream-python-runtime.wasm";

/// Default Python compiled WASM cache filename
pub const DEFAULT_PYTHON_CWASM_FILENAME: &str = "functionstream-python-runtime.cwasm";

fn default_python_wasm_path() -> String {
    format!("{}/{}", DEFAULT_PYTHON_CACHE_DIR, DEFAULT_PYTHON_WASM_FILENAME)
}

fn default_python_cache_dir() -> String {
    DEFAULT_PYTHON_CACHE_DIR.to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonConfig {
    /// Path to the Python WASM file
    /// Default: data/cache/python-runner/functionstream-python-runtime.wasm
    #[serde(default = "default_python_wasm_path")]
    pub wasm_path: String,
    
    /// Cache directory for precompiled components
    /// Default: data/cache/python-runner
    #[serde(default = "default_python_cache_dir")]
    pub cache_dir: String,
    
    /// Enable component caching
    /// If true, precompiled components will be cached to speed up subsequent loads
    /// Default: true
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
    /// Get the WASM file path as PathBuf
    pub fn wasm_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.wasm_path)
    }
    
    /// Get the cache directory as PathBuf
    pub fn cache_dir_buf(&self) -> PathBuf {
        PathBuf::from(&self.cache_dir)
    }
    
    /// Get the precompiled component cache file path (cwasm)
    pub fn cwasm_cache_path(&self) -> PathBuf {
        self.cache_dir_buf().join(DEFAULT_PYTHON_CWASM_FILENAME)
    }
}
