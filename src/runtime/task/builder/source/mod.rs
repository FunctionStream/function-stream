// Source Builder - Source type task builder
//
// Specifically handles building logic for Source type configuration (future support)

use crate::runtime::processor::WASM::wasm_task::WasmTask;
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
    /// - `wasm_path`: WASM module path (optional)
    ///
    /// # Returns
    /// - `Ok(Arc<WasmTask>)`: Successfully created task (future support)
    /// - `Err(...)`: Currently not implemented, returns error
    pub fn build(
        _task_name: String,
        yaml_value: &Value,
        _wasm_path: String,
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
