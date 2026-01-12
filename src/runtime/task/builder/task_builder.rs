// Task Builder - Build WASM Task from configuration
//
// Provides factory methods to create complete WasmTask from YAML configuration
// Calls corresponding builders based on configuration type (Processor, Source, Sink)

use crate::runtime::task::TaskLifecycle;
use crate::runtime::task::builder::processor::ProcessorBuilder;
use crate::runtime::task::builder::sink::SinkBuilder;
use crate::runtime::task::builder::source::SourceBuilder;
use crate::runtime::task::yaml_keys::{NAME, TYPE, type_values};
use serde_yaml::Value;
use std::fs;
use std::sync::Arc;

/// TaskBuilder - Build WasmTask from configuration
///
/// Calls corresponding builders based on configuration type (Processor, Source, Sink)
pub struct TaskBuilder;

impl TaskBuilder {
    /// Create TaskLifecycle from configuration byte array
    ///
    /// Based on the `type` field in the configuration file, calls the corresponding builder:
    /// - `processor`: Calls `ProcessorBuilder`
    /// - `source`: Calls `SourceBuilder` (future support)
    /// - `sink`: Calls `SinkBuilder` (future support)
    ///
    /// # Arguments
    /// - `config_bytes`: Configuration file byte array (YAML format)
    /// - `wasm_bytes`: WASM binary package byte array
    ///
    /// # Returns
    /// - `Ok(Box<dyn TaskLifecycle>)`: Successfully created TaskLifecycle
    /// - `Err(...)`: Creation failed
    /// Create TaskLifecycle from YAML configuration
    ///
    /// # Arguments
    /// - `config_bytes`: Configuration file byte array (YAML format)
    /// - `wasm_bytes`: WASM binary package byte array
    ///
    /// # Returns
    /// - `Ok(Box<dyn TaskLifecycle>)`: Successfully created TaskLifecycle
    /// - `Err(...)`: Creation failed
    pub fn from_yaml_config(
        config_bytes: &[u8],
        wasm_bytes: &[u8],
    ) -> Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>> {
        log::debug!(
            "TaskBuilder::from_yaml_config: config size={} bytes, wasm size={} bytes",
            config_bytes.len(),
            wasm_bytes.len()
        );

        // 1. Parse YAML configuration
        log::debug!("Step 1: Parsing YAML config");
        let yaml_value: Value = serde_yaml::from_slice(config_bytes).map_err(
            |e| -> Box<dyn std::error::Error + Send> {
                let config_preview = String::from_utf8_lossy(config_bytes);
                let preview = config_preview.chars().take(500).collect::<String>();
                log::error!(
                    "Failed to parse YAML config. Error: {}. Config preview (first 500 chars):\n{}",
                    e,
                    preview
                );
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Failed to parse YAML config: {}. Config preview: {}",
                        e, preview
                    ),
                ))
            },
        )?;

        // 2. Extract task name from YAML
        log::debug!("Step 2: Extracting task name from YAML");
        let task_name = yaml_value
            .get(NAME)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                let available_keys: Vec<String> = yaml_value
                    .as_mapping()
                    .map(|m| {
                        m.keys()
                            .filter_map(|k| k.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                log::error!(
                    "Missing '{}' field in YAML config. Available keys: {:?}",
                    NAME,
                    available_keys
                );
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Missing '{}' field in YAML config. Available keys: {:?}",
                        NAME, available_keys
                    ),
                )) as Box<dyn std::error::Error + Send>
            })?
            .to_string();
        log::debug!("Task name extracted: '{}'", task_name);

        // 3. Validate configuration type
        log::debug!("Step 3: Validating config type");
        let config_type_str = yaml_value
            .get(TYPE)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                log::error!(
                    "Missing '{}' field in YAML config for task '{}'",
                    TYPE,
                    task_name
                );
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Missing '{}' field in YAML config for task '{}'",
                        TYPE, task_name
                    ),
                )) as Box<dyn std::error::Error + Send>
            })?;
        log::debug!("Config type: '{}'", config_type_str);

        // 4. Create temporary directory and write WASM file
        // Use task name and timestamp to create unique directory name
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("wasm-task-{}-{}", task_name, timestamp));
        fs::create_dir_all(&temp_dir).map_err(|e| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::other(
                format!("Failed to create temporary directory: {}", e),
            ))
        })?;

        let wasm_path = temp_dir.join("module.wasm");
        fs::write(&wasm_path, wasm_bytes).map_err(|e| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::other(
                format!("Failed to write WASM file: {}", e),
            ))
        })?;

        let wasm_path_str = wasm_path.to_string_lossy().to_string();

        // 5. Call corresponding builder based on configuration type
        log::debug!(
            "Step 5: Building task based on config type '{}'",
            config_type_str
        );
        let task: Box<dyn TaskLifecycle> = match config_type_str {
            type_values::PROCESSOR => {
                log::debug!("Building processor task '{}'", task_name);
                let wasm_task =
                    ProcessorBuilder::build(task_name.clone(), &yaml_value, wasm_path_str)
                        .map_err(|e| -> Box<dyn std::error::Error + Send> {
                            log::error!(
                                "ProcessorBuilder::build failed for task '{}': {}",
                                task_name,
                                e
                            );
                            e
                        })?;
                // Convert Arc<WasmTask> to Box<dyn TaskLifecycle>
                // Since WasmTask implements TaskLifecycle, we can convert directly
                log::debug!("Unwrapping Arc<WasmTask> for task '{}'", task_name);
                Box::new(Arc::try_unwrap(wasm_task)
                    .map_err(|arc| -> Box<dyn std::error::Error + Send> {
                        log::error!("Failed to unwrap Arc<WasmTask> for task '{}': Arc has {} strong references", 
                            task_name, Arc::strong_count(&arc));
                        Box::new(std::io::Error::other(
                            format!("Failed to unwrap Arc<WasmTask> for task '{}': Arc has {} strong references", 
                                task_name, Arc::strong_count(&arc)),
                        ))
                    })?)
            }
            type_values::SOURCE => {
                let wasm_task =
                    SourceBuilder::build(task_name.clone(), &yaml_value, wasm_path_str)?;
                Box::new(Arc::try_unwrap(wasm_task).map_err(
                    |_| -> Box<dyn std::error::Error + Send> {
                        Box::new(std::io::Error::other(
                            "Failed to unwrap Arc<WasmTask>",
                        ))
                    },
                )?)
            }
            type_values::SINK => {
                let wasm_task = SinkBuilder::build(task_name.clone(), &yaml_value, wasm_path_str)?;
                Box::new(Arc::try_unwrap(wasm_task).map_err(
                    |_| -> Box<dyn std::error::Error + Send> {
                        Box::new(std::io::Error::other(
                            "Failed to unwrap Arc<WasmTask>",
                        ))
                    },
                )?)
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unsupported config type: {}", config_type_str),
                )) as Box<dyn std::error::Error + Send>);
            }
        };

        // Note: temp_dir will remain for the lifetime of the task
        // Since WasmTask will hold wasm_path, we need to ensure the path is valid during the task's lifetime
        // A better approach would be to store temp_dir in WasmTask, but this requires modifying the WasmTask structure
        // For now, keep the directory existing, can be optimized later

        Ok(task)
    }
}
