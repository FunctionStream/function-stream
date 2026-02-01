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

//! Task Builder - Factory for creating tasks from YAML configuration
//!
//! Provides unified factory methods to create TaskLifecycle instances from YAML config.
//! Dispatches to specific builders (Processor, Source, Sink, Python) based on task type.

use crate::runtime::task::builder::processor::ProcessorBuilder;
#[cfg(feature = "python")]
use crate::runtime::task::builder::python::PythonBuilder;
use crate::runtime::task::builder::sink::SinkBuilder;
use crate::runtime::task::builder::source::SourceBuilder;
use crate::runtime::task::yaml_keys::{NAME, TYPE, type_values};
use crate::runtime::task::TaskLifecycle;
use serde_yaml::Value;
use std::sync::Arc;

/// Type alias for builder results
pub type BuildResult = Result<Box<dyn TaskLifecycle>, Box<dyn std::error::Error + Send>>;

/// Task builder error
fn build_error(msg: impl Into<String>) -> Box<dyn std::error::Error + Send> {
    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into()))
}

/// Factory for creating tasks from configuration
pub struct TaskBuilder;

impl TaskBuilder {
    /// Create a task from YAML configuration and module bytes
    ///
    /// # Arguments
    /// * `config_bytes` - YAML configuration as bytes
    /// * `module_bytes` - WASM/Python module bytes
    ///
    /// # Returns
    /// A boxed TaskLifecycle implementation based on the task type in config
    pub fn from_yaml_config(config_bytes: &[u8], module_bytes: &[u8]) -> BuildResult {
        let yaml_value = Self::parse_yaml(config_bytes)?;
        let task_name = Self::extract_task_name(&yaml_value)?;
        let task_type = Self::extract_task_type(&yaml_value, &task_name)?;

        Self::build_task(&task_type, task_name, &yaml_value, module_bytes.to_vec())
    }

    /// Create a Python task from YAML configuration (for fs-exec)
    ///
    /// This method is specifically for Python functions executed via fs-exec.
    /// It forces the task type to Python regardless of the config's type field.
    ///
    /// # Arguments
    /// * `config_bytes` - YAML configuration as bytes
    /// * `modules` - Python modules as (name, bytes) pairs
    #[cfg(feature = "python")]
    pub fn from_python_config(config_bytes: &[u8], modules: &[(String, Vec<u8>)]) -> BuildResult {
        let yaml_value = Self::parse_yaml(config_bytes)?;
        let task_name = Self::extract_task_name(&yaml_value)?;

        log::debug!("Creating Python task '{}' via fs-exec", task_name);
        PythonBuilder::build(task_name, &yaml_value, modules)
    }

    /// Parse YAML configuration
    fn parse_yaml(config_bytes: &[u8]) -> Result<Value, Box<dyn std::error::Error + Send>> {
        serde_yaml::from_slice(config_bytes).map_err(|e| {
            let preview: String = String::from_utf8_lossy(config_bytes).chars().take(500).collect();
            log::error!("Failed to parse YAML config: {}. Preview:\n{}", e, preview);
            build_error(format!("Failed to parse YAML: {}", e))
        })
    }

    /// Extract task name from YAML
    fn extract_task_name(yaml: &Value) -> Result<String, Box<dyn std::error::Error + Send>> {
        yaml.get(NAME)
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| {
                let keys = Self::get_yaml_keys(yaml);
                log::error!("Missing '{}' field. Available keys: {:?}", NAME, keys);
                build_error(format!("Missing '{}' field in config", NAME))
            })
    }

    /// Extract task type from YAML
    fn extract_task_type(yaml: &Value, task_name: &str) -> Result<String, Box<dyn std::error::Error + Send>> {
        yaml.get(TYPE)
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| {
                log::error!("Missing '{}' field for task '{}'", TYPE, task_name);
                build_error(format!("Missing '{}' field for task '{}'", TYPE, task_name))
            })
    }

    /// Get available keys from YAML for error messages
    fn get_yaml_keys(yaml: &Value) -> Vec<String> {
        yaml.as_mapping()
            .map(|m| m.keys().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default()
    }

    /// Build task based on type
    fn build_task(
        task_type: &str,
        task_name: String,
        yaml: &Value,
        module_bytes: Vec<u8>,
    ) -> BuildResult {
        match task_type {
            type_values::PROCESSOR => Self::build_wasm_task(
                ProcessorBuilder::build(task_name.clone(), yaml, module_bytes),
                &task_name,
            ),
            type_values::SOURCE => Self::build_wasm_task(
                SourceBuilder::build(task_name.clone(), yaml, module_bytes),
                &task_name,
            ),
            type_values::SINK => Self::build_wasm_task(
                SinkBuilder::build(task_name.clone(), yaml, module_bytes),
                &task_name,
            ),
            _ => {
                log::error!("Unsupported task type: {}", task_type);
                Err(build_error(format!("Unsupported task type: {}", task_type)))
            }
        }
    }

    /// Build and unwrap WASM task from Arc
    fn build_wasm_task(
        result: Result<Arc<crate::runtime::processor::wasm::wasm_task::WasmTask>, Box<dyn std::error::Error + Send>>,
        task_name: &str,
    ) -> BuildResult {
        let arc = result.map_err(|e| {
            log::error!("Failed to build task '{}': {}", task_name, e);
            e
        })?;

        Arc::try_unwrap(arc)
            .map(|task| Box::new(task) as Box<dyn TaskLifecycle>)
            .map_err(|arc| {
                let refs = Arc::strong_count(&arc);
                log::error!("Failed to unwrap Arc for '{}': {} references", task_name, refs);
                build_error(format!("Task '{}' has {} active references", task_name, refs))
            })
    }
}
