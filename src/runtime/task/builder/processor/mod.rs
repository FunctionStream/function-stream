// Processor Builder - Processor type task builder
//
// Specifically handles building logic for Processor type configuration

use crate::runtime::input::{InputSource, InputSourceProvider};
use crate::runtime::output::{OutputSink, OutputSinkProvider};
use crate::runtime::processor::WASM::wasm_processor::WasmProcessorImpl;
use crate::runtime::processor::WASM::wasm_processor_trait::WasmProcessor;
use crate::runtime::processor::WASM::wasm_task::{TaskEnvironment, WasmTask};
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
    /// - `wasm_path`: WASM module path
    ///
    /// # Returns
    /// - `Ok(Arc<WasmTask>)`: Successfully created WasmTask
    /// - `Err(...)`: Creation failed
    pub fn build(
        task_name: String,
        yaml_value: &Value,
        wasm_path: String,
    ) -> Result<Arc<WasmTask>, Box<dyn std::error::Error + Send>> {
        // 1. Validate configuration type
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

        // 2. Parse as WasmTaskConfig
        let task_config = WasmTaskConfig::from_yaml_value(task_name.clone(), yaml_value)?;

        // 3. Calculate total number of input sources
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

        // 4. Create task environment
        let environment = TaskEnvironment::new(task_config.task_name.clone());

        // 5. Create InputSource instances first
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
            log::info!(
                "Created {} input source(s) for input group #{}",
                group_inputs.len(),
                group_idx + 1
            );
            all_inputs.extend(group_inputs);
        }
        log::info!(
            "Created {} total input source(s) from {} input group(s)",
            all_inputs.len(),
            task_config.input_groups.len()
        );

        // 6. Create OutputSink instances first (create only one copy)
        let outputs = Self::create_outputs_from_config(&task_config.outputs)?;
        log::info!("Created {} output(s)", outputs.len());

        // 7. Verify WASM file exists
        if !std::path::Path::new(&wasm_path).exists() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("WASM file not found: {}", wasm_path),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Get file size for logging
        let wasm_size = std::fs::metadata(&wasm_path)
            .map_err(|e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to get WASM file metadata {}: {}", wasm_path, e),
                ))
            })?
            .len();
        log::info!("WASM file size: {} MB", wasm_size / 1024 / 1024);

        // 8. Create Processor instance from ProcessorConfig (pass path instead of byte array)
        let processor =
            Self::create_processor_from_config(&task_config.processor, wasm_path.clone())?;
        log::info!("Created WASM processor: {}", task_config.processor.name);

        // 9. Create WasmTask (only pass outputs, use clone instead of recreating)
        let task = WasmTask::new(environment, all_inputs, processor, outputs);
        let task = Arc::new(task);

        log::info!(
            "WasmTask created successfully for processor task: {}",
            task_config.task_name
        );

        Ok(task)
    }

    /// Create InputSource instances from InputConfig list
    fn create_inputs_from_config(
        inputs: &[InputConfig],
        group_idx: usize,
    ) -> Result<Vec<Box<dyn InputSource>>, Box<dyn std::error::Error + Send>> {
        InputSourceProvider::from_input_configs(inputs, group_idx)
    }

    /// Create Processor instance from ProcessorConfig
    ///
    /// Note: processor is not initialized here, but initialized uniformly in WasmTask::init_with_context
    fn create_processor_from_config(
        processor_config: &ProcessorConfig,
        wasm_path: String,
    ) -> Result<Box<dyn WasmProcessor>, Box<dyn std::error::Error + Send>> {
        let processor_impl = WasmProcessorImpl::new(
            processor_config.name.clone(),
            wasm_path,
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
