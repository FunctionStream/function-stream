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

// Processor Builder - Processor type task builder
//
// Specifically handles building logic for Processor type configuration

use crate::runtime::input::{InputSource, InputSourceProvider};
use crate::runtime::output::{OutputSink, OutputSinkProvider};
use crate::runtime::processor::wasm::wasm_processor::WasmProcessorImpl;
use crate::runtime::processor::wasm::wasm_processor_trait::WasmProcessor;
use crate::runtime::processor::wasm::wasm_task::WasmTask;
use crate::runtime::task::yaml_keys::{TYPE, type_values};
use crate::runtime::task::{InputConfig, OutputConfig, ProcessorConfig, WasmTaskConfig};
use serde_yaml::Value;
use std::sync::Arc;

/// ProcessorBuilder - Processor type task builder
pub struct ProcessorBuilder;

impl ProcessorBuilder {
    /// Create Processor type WasmTask from YAML configuration
    ///
    /// # Arguments
    /// - `task_name`: Task name
    /// - `yaml_value`: YAML configuration value (root-level configuration)
    /// - `wasm_path`: wasm module path
    ///
    /// # Returns
    /// - `Ok(Arc<WasmTask>)`: Successfully created WasmTask
    /// - `Err(...)`: Creation failed
    pub fn build(
        task_name: String,
        yaml_value: &Value,
        module_bytes: Vec<u8>,
        create_time: u64,
    ) -> Result<Arc<WasmTask>, Box<dyn std::error::Error + Send>> {
        let config_type = yaml_value
            .get(TYPE)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Missing '{}' field in YAML config", TYPE),
                )) as Box<dyn std::error::Error + Send>
            })?;

        if config_type != type_values::PROCESSOR {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid config type '{}', expected '{}'",
                    config_type,
                    type_values::PROCESSOR
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        let task_config = WasmTaskConfig::from_yaml_value(task_name.clone(), yaml_value)?;

        let total_inputs: usize = task_config
            .input_groups
            .iter()
            .map(|group| group.inputs.len())
            .sum();

        log::info!(
            "Parsed processor config: {} input groups ({} total inputs), {} outputs, processor: {}",
            task_config.input_groups.len(),
            total_inputs,
            task_config.outputs.len(),
            task_config.processor.name
        );

        let mut all_inputs = Vec::new();
        for (group_idx, input_group) in task_config.input_groups.iter().enumerate() {
            let group_inputs = Self::create_inputs_from_config(&input_group.inputs, group_idx)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Failed to create input sources for input group #{}: {}",
                            group_idx + 1,
                            e
                        ),
                    ))
                })?;
            log::debug!(
                "Created {} input source(s) for input group #{}",
                group_inputs.len(),
                group_idx + 1
            );
            all_inputs.extend(group_inputs);
        }
        log::debug!(
            "Created {} total input source(s) from {} input group(s)",
            all_inputs.len(),
            task_config.input_groups.len()
        );

        let outputs = Self::create_outputs_from_config(&task_config.outputs)?;
        log::debug!("Created {} output(s)", outputs.len());

        let processor = Self::create_processor_from_config(&task_config.processor, module_bytes)?;
        log::debug!("Created wasm processor: {}", task_config.processor.name);

        let task = WasmTask::new(
            task_config.task_name.clone(),
            type_values::PROCESSOR.to_string(),
            all_inputs,
            processor,
            outputs,
            create_time,
        );
        let task = Arc::new(task);

        log::debug!(
            "WasmTask created successfully for processor task: {}",
            task_config.task_name
        );

        Ok(task)
    }

    fn create_inputs_from_config(
        inputs: &[InputConfig],
        group_idx: usize,
    ) -> Result<Vec<Box<dyn InputSource>>, Box<dyn std::error::Error + Send>> {
        InputSourceProvider::from_input_configs(inputs, group_idx)
    }

    fn create_processor_from_config(
        processor_config: &ProcessorConfig,
        module_bytes: Vec<u8>,
    ) -> Result<Box<dyn WasmProcessor>, Box<dyn std::error::Error + Send>> {
        let processor_impl = WasmProcessorImpl::new(
            processor_config.name.clone(),
            module_bytes,
            processor_config.init_config.clone(),
        );

        Ok(Box::new(processor_impl))
    }

    /// Create OutputSink instances from OutputConfig list
    fn create_outputs_from_config(
        outputs: &[OutputConfig],
    ) -> Result<Vec<Box<dyn OutputSink>>, Box<dyn std::error::Error + Send>> {
        OutputSinkProvider::from_output_configs(outputs)
    }
}
