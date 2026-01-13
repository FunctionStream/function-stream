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

// Task Builder - Build WASM Task from configuration
//
// Provides factory methods to create complete WasmTask from YAML configuration
// Calls corresponding builders based on configuration type (Processor, Source, Sink)

use crate::runtime::task::TaskLifecycle;
use crate::runtime::task::builder::processor::ProcessorBuilder;
use crate::runtime::task::builder::python::PythonBuilder;
use crate::runtime::task::builder::sink::SinkBuilder;
use crate::runtime::task::builder::source::SourceBuilder;
use crate::runtime::task::yaml_keys::{NAME, TYPE, type_values};
use serde_yaml::Value;
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

        let module_bytes = wasm_bytes.to_vec();

        let task: Box<dyn TaskLifecycle> = match config_type_str {
            type_values::PROCESSOR => {
                Self::build_processor_task(task_name.clone(), &yaml_value, module_bytes)?
            }
            type_values::SOURCE => {
                Self::build_source_task(task_name.clone(), &yaml_value, module_bytes)?
            }
            type_values::SINK => {
                Self::build_sink_task(task_name.clone(), &yaml_value, module_bytes)?
            }
            type_values::PYTHON => {
                Self::build_python_task(task_name.clone(), &yaml_value, module_bytes)?
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unsupported config type: {}", config_type_str),
                )) as Box<dyn std::error::Error + Send>);
            }
        };

        Ok(task)
    }

    fn build_processor_task(
        task_name: String,
        yaml_value: &Value,
        module_bytes: Vec<u8>,
    ) -> Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>> {
        let wasm_task = ProcessorBuilder::build(task_name.clone(), yaml_value, module_bytes)
            .map_err(|e| -> Box<dyn std::error::Error + Send> {
                log::error!(
                    "ProcessorBuilder::build failed for task '{}': {}",
                    task_name,
                    e
                );
                e
            })?;
        Ok(Box::new(Arc::try_unwrap(wasm_task).map_err(|arc| -> Box<dyn std::error::Error + Send> {
            log::error!(
                "Failed to unwrap Arc<WasmTask> for task '{}': Arc has {} strong references",
                task_name,
                Arc::strong_count(&arc)
            );
            Box::new(std::io::Error::other(format!(
                "Failed to unwrap Arc<WasmTask> for task '{}': Arc has {} strong references",
                task_name,
                Arc::strong_count(&arc)
            )))
        })?))
    }

    fn build_source_task(
        task_name: String,
        yaml_value: &Value,
        module_bytes: Vec<u8>,
    ) -> Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>> {
        let wasm_task = SourceBuilder::build(task_name, yaml_value, module_bytes)?;
        Ok(Box::new(Arc::try_unwrap(wasm_task).map_err(|_| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::other("Failed to unwrap Arc<WasmTask>"))
        })?))
    }

    fn build_sink_task(
        task_name: String,
        yaml_value: &Value,
        module_bytes: Vec<u8>,
    ) -> Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>> {
        let wasm_task = SinkBuilder::build(task_name, yaml_value, module_bytes)?;
        Ok(Box::new(Arc::try_unwrap(wasm_task).map_err(|_| -> Box<dyn std::error::Error + Send> {
            Box::new(std::io::Error::other("Failed to unwrap Arc<WasmTask>"))
        })?))
    }


    fn build_python_task(
        task_name: String,
        yaml_value: &Value,
        module_bytes: Vec<u8>,
    ) -> Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>> {
        PythonBuilder::build(task_name, yaml_value, module_bytes)
    }
}
