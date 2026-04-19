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
use serde_yaml::Value;
use uuid::Uuid;

use crate::config::log_config::LogConfig;
use crate::config::python_config::PythonConfig;
use crate::config::service_config::ServiceConfig;
use crate::config::streaming_job::{ResolvedStreamingJobConfig, StreamingJobConfig};
use crate::config::wasm_config::WasmConfig;

/// Default for [`StreamingConfig::streaming_runtime_memory_bytes`] when unset. **200 MiB.**
pub const DEFAULT_STREAMING_RUNTIME_MEMORY_BYTES: u64 = 200 * 1024 * 1024;

/// Default for [`StreamingConfig::operator_state_store_memory_bytes`] when unset. **100 MiB.**
pub const DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamingConfig {
    #[serde(flatten)]
    pub job: StreamingJobConfig,
    /// Bytes reserved in the global memory pool for streaming execution (pipeline buffers,
    /// batch collect, backpressure).
    #[serde(default)]
    pub streaming_runtime_memory_bytes: Option<u64>,
    /// Per stateful operator: in-memory state store cap before spill.
    #[serde(default)]
    pub operator_state_store_memory_bytes: Option<u64>,
}

impl StreamingConfig {
    #[inline]
    pub fn resolved_job(&self) -> ResolvedStreamingJobConfig {
        self.job.resolve()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    pub service: ServiceConfig,
    pub logging: LogConfig,
    #[serde(default)]
    pub python: PythonConfig,
    #[serde(default)]
    pub wasm: WasmConfig,
    #[serde(default)]
    pub state_storage: crate::config::storage::StateStorageConfig,
    #[serde(default)]
    pub task_storage: crate::config::storage::TaskStorageConfig,
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub stream_catalog: crate::config::storage::StreamCatalogConfig,
}

impl GlobalConfig {
    pub fn from_cargo() -> Self {
        let mut config = Self::default();
        config.service.version = env!("CARGO_PKG_VERSION").to_string();
        config.service.service_name = Uuid::new_v4().to_string();
        config
    }

    pub fn cargo_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_yaml_value(value: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let config: GlobalConfig = serde_yaml::from_value(value)?;
        Ok(config)
    }

    pub fn service_id(&self) -> &str {
        &self.service.service_id
    }

    pub fn port(&self) -> u16 {
        self.service.port
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.service.port == 0 {
            return Err(format!("Invalid port: {}", self.service.port));
        }
        Ok(())
    }

    pub fn load<P: AsRef<std::path::Path>>(
        path: Option<P>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = path
            .map(|p| p.as_ref().to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("../../conf/config.yaml"));

        if config_path.exists() {
            crate::config::load_global_config(&config_path)
        } else {
            Ok(Self::default())
        }
    }
}
