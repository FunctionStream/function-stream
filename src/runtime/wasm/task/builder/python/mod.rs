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

// python Builder - python runtime task builder
//
// Specifically handles building logic for python runtime configuration

use crate::runtime::input::{Input, InputProvider};
use crate::runtime::output::{Output, OutputProvider};
use crate::runtime::processor::python::get_python_engine_and_component;
use crate::runtime::processor::wasm::wasm_processor::WasmProcessorImpl;
use crate::runtime::processor::wasm::wasm_processor_trait::WasmProcessor;
use crate::runtime::processor::wasm::wasm_task::WasmTask;
use crate::runtime::wasm::task::yaml_keys::{TYPE, type_values};
use crate::runtime::wasm::task::{InputConfig, OutputConfig, ProcessorConfig, WasmTaskConfig};
use serde_yaml::Value;
use std::sync::Arc;

pub struct PythonBuilder;

impl PythonBuilder {
    pub fn build(
        task_name: String,
        yaml_value: &Value,
        modules: &[(String, Vec<u8>)],
        create_time: u64,
    ) -> Result<Box<dyn crate::runtime::wasm::task::TaskLifecycle>, Box<dyn std::error::Error + Send>>
    {
        let config_type = yaml_value
            .get(TYPE)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Missing '{}' field in YAML config", TYPE),
                )) as Box<dyn std::error::Error + Send>
            })?;

        if config_type != type_values::PYTHON {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid config type '{}', expected '{}'",
                    config_type,
                    type_values::PYTHON
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        let task_config = WasmTaskConfig::from_yaml_value(task_name.clone(), yaml_value)?;

        let total_inputs: usize = task_config
            .input_groups
            .iter()
            .map(|group| group.inputs.len())
            .sum();

        log::debug!(
            "Parsed python config: {} input groups ({} total inputs), {} outputs, processor: {}",
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

        let processor = Self::create_processor_from_config(&task_config.processor, modules)?;
        log::debug!("Created python processor: {}", task_config.processor.name);

        let task = WasmTask::new(
            task_config.task_name.clone(),
            type_values::PYTHON.to_string(),
            task_config.processor.input_selector.clone(),
            task_config.processor.runtime.clone(),
            all_inputs,
            processor,
            outputs,
            create_time,
        );
        let task = Arc::new(task);

        log::debug!(
            "WasmTask created successfully for python task: {}",
            task_config.task_name
        );

        Ok(Box::new(Arc::try_unwrap(task).map_err(
            |_| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::other("Failed to unwrap Arc<WasmTask>"))
            },
        )?))
    }

    fn create_inputs_from_config(
        inputs: &[InputConfig],
        group_idx: usize,
    ) -> Result<Vec<Box<dyn Input>>, Box<dyn std::error::Error + Send>> {
        InputProvider::from_input_configs(inputs, group_idx)
    }

    fn create_processor_from_config(
        processor_config: &ProcessorConfig,
        modules: &[(String, Vec<u8>)],
    ) -> Result<Box<dyn WasmProcessor>, Box<dyn std::error::Error + Send>> {
        // Get python wasm engine and component for reuse
        let (custom_engine, custom_component) = get_python_engine_and_component().map_err(
            |e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::other(format!(
                    "Failed to get python wasm engine and component: {}",
                    e
                )))
            },
        )?;

        // Clone the Component (Component implements Clone via Arc<ComponentInner>)
        let processor_impl = WasmProcessorImpl::new_with_custom_engine_and_component(
            processor_config.name.clone(),
            modules,
            processor_config.init_config.clone(),
            custom_engine,
            (*custom_component).clone(),
        );

        Ok(Box::new(processor_impl))
    }

    fn create_outputs_from_config(
        outputs: &[OutputConfig],
    ) -> Result<Vec<Box<dyn Output>>, Box<dyn std::error::Error + Send>> {
        OutputProvider::from_output_configs(outputs)
    }
}
