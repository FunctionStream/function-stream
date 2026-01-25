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

// Source Builder - Source type task builder
//
// Specifically handles building logic for Source type configuration (future support)

use crate::runtime::processor::wasm::wasm_task::WasmTask;
use crate::runtime::task::yaml_keys::{TYPE, type_values};
use serde_yaml::Value;
use std::sync::Arc;

/// SourceBuilder - Source type task builder
pub struct SourceBuilder;

impl SourceBuilder {
    /// Create Source type task from YAML configuration
    ///
    /// # Arguments
    /// - `task_name`: Task name
    /// - `yaml_value`: YAML configuration value (root-level configuration)
    /// - `wasm_path`: wasm module path (optional)
    ///
    /// # Returns
    /// - `Ok(Arc<WasmTask>)`: Successfully created task (future support)
    /// - `Err(...)`: Currently not implemented, returns error
    pub fn build(
        _task_name: String,
        yaml_value: &Value,
        _module_bytes: Vec<u8>,
    ) -> Result<Arc<WasmTask>, Box<dyn std::error::Error + Send>> {
        // Validate configuration type
        let config_type = yaml_value
            .get(TYPE)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Missing '{}' field in YAML config", TYPE),
                )) as Box<dyn std::error::Error + Send>
            })?;

        if config_type != type_values::SOURCE {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid config type '{}', expected '{}'",
                    config_type,
                    type_values::SOURCE
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        // TODO: Implement Source type task building logic
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Source type task builder is not yet implemented",
        )) as Box<dyn std::error::Error + Send>)
    }
}
