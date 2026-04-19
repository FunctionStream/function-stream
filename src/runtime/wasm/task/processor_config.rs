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

//! Task Configuration - Configuration structs for task components
//!
//! Defines configuration structures for Input, Processor, and Output components.

use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct InputRuntimeConfig {
    #[serde(default = "input_default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "input_default_batch_consume_size")]
    pub batch_consume_size: usize,
    #[serde(default = "input_default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    #[serde(default = "input_default_sleep_when_full_ms")]
    pub sleep_when_full_ms: u64,
}

fn input_default_channel_capacity() -> usize {
    10000
}
fn input_default_batch_consume_size() -> usize {
    10000
}
fn input_default_poll_timeout_ms() -> u64 {
    1000
}
fn input_default_sleep_when_full_ms() -> u64 {
    10
}

impl Default for InputRuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: input_default_channel_capacity(),
            batch_consume_size: input_default_batch_consume_size(),
            poll_timeout_ms: input_default_poll_timeout_ms(),
            sleep_when_full_ms: input_default_sleep_when_full_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ProcessorRuntimeConfig {
    #[serde(default = "processor_default_max_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "processor_default_max_idle_count")]
    pub max_idle_count: u64,
    #[serde(default = "processor_default_idle_sleep_ms")]
    pub idle_sleep_ms: u64,
    #[serde(default = "processor_default_control_timeout_ms")]
    pub control_timeout_ms: u64,
    #[serde(default = "processor_default_control_max_retries")]
    pub control_max_retries: u32,
}

fn processor_default_max_batch_size() -> usize {
    1000
}
fn processor_default_max_idle_count() -> u64 {
    10000
}
fn processor_default_idle_sleep_ms() -> u64 {
    500
}
fn processor_default_control_timeout_ms() -> u64 {
    5000
}
fn processor_default_control_max_retries() -> u32 {
    3
}

impl Default for ProcessorRuntimeConfig {
    fn default() -> Self {
        Self {
            max_batch_size: processor_default_max_batch_size(),
            max_idle_count: processor_default_max_idle_count(),
            idle_sleep_ms: processor_default_idle_sleep_ms(),
            control_timeout_ms: processor_default_control_timeout_ms(),
            control_max_retries: processor_default_control_max_retries(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OutputRuntimeConfig {
    #[serde(default = "output_default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "output_default_batch_consume_size")]
    pub batch_consume_size: usize,
}

fn output_default_channel_capacity() -> usize {
    10000
}
fn output_default_batch_consume_size() -> usize {
    10000
}

impl Default for OutputRuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: output_default_channel_capacity(),
            batch_consume_size: output_default_batch_consume_size(),
        }
    }
}

// ============================================================================
// Input Configuration
// ============================================================================

/// InputConfig - Input source configuration
///
/// Supports multiple input source types, each with its corresponding configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "input-type", rename_all = "kebab-case")]
pub enum InputConfig {
    Kafka {
        bootstrap_servers: String,
        topic: String,
        #[serde(default)]
        partition: Option<u32>,
        group_id: String,
        #[serde(flatten)]
        extra: HashMap<String, String>,
        #[serde(default)]
        runtime: InputRuntimeConfig,
    },
}

impl InputConfig {
    /// Parse InputConfig from YAML Value
    ///
    /// # Arguments
    /// - `value`: YAML Value object
    ///
    /// # Returns
    /// - `Ok(InputConfig)`: Successfully parsed
    /// - `Err(...)`: Parsing failed
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // Try to deserialize using serde
        let config: InputConfig = serde_yaml::from_value(value.clone()).map_err(
            |e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse input config: {}", e),
                ))
            },
        )?;
        Ok(config)
    }

    /// Get input source type name
    pub fn input_type(&self) -> &'static str {
        match self {
            InputConfig::Kafka { .. } => "kafka",
        }
    }

    pub fn input_runtime_config(&self) -> InputRuntimeConfig {
        match self {
            InputConfig::Kafka { runtime, .. } => runtime.clone(),
        }
    }
}

// ============================================================================
// Input Group Configuration
// ============================================================================

/// InputGroup - Input group configuration
///
/// An input group contains multiple input source configurations.
/// A wasm task can contain multiple input groups, each group can contain multiple input sources.
#[derive(Debug, Clone)]
pub struct InputGroup {
    /// Input source configuration list
    ///
    /// An input group can contain multiple input sources that are processed together.
    pub inputs: Vec<InputConfig>,
}

impl InputGroup {
    /// Create a new input group
    ///
    /// # Arguments
    /// - `inputs`: Input source configuration list
    ///
    /// # Returns
    /// - `InputGroup`: The created input group
    pub fn new(inputs: Vec<InputConfig>) -> Self {
        Self { inputs }
    }

    /// Parse InputGroup from YAML Value
    ///
    /// # Arguments
    /// - `value`: YAML Value object (should be an object containing an inputs array)
    ///
    /// # Returns
    /// - `Ok(InputGroup)`: Successfully parsed
    /// - `Err(...)`: Parsing failed
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // If value itself is an array, parse directly as input source list
        if let Some(inputs_seq) = value.as_sequence() {
            let mut inputs = Vec::new();
            for (idx, input_value) in inputs_seq.iter().enumerate() {
                let input_type = input_value
                    .get("input-type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                let input = InputConfig::from_yaml_value(input_value).map_err(
                    |e| -> Box<dyn std::error::Error + Send> {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Failed to parse input #{} (type: {}) in input group: {}",
                                idx + 1,
                                input_type,
                                e
                            ),
                        ))
                    },
                )?;

                let parsed_type = input.input_type();
                if parsed_type != input_type && input_type != "unknown" {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Input #{} type mismatch in input group: expected '{}', but got '{}'",
                            idx + 1,
                            input_type,
                            parsed_type
                        ),
                    )) as Box<dyn std::error::Error + Send>);
                }

                inputs.push(input);
            }
            return Ok(InputGroup::new(inputs));
        }

        // If value is an object, try to get the inputs field
        if let Some(inputs_value) = value.get("inputs")
            && let Some(inputs_seq) = inputs_value.as_sequence()
        {
            let mut inputs = Vec::new();
            for (idx, input_value) in inputs_seq.iter().enumerate() {
                let input_type = input_value
                    .get("input-type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                let input = InputConfig::from_yaml_value(input_value).map_err(
                    |e| -> Box<dyn std::error::Error + Send> {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Failed to parse input #{} (type: {}) in input group: {}",
                                idx + 1,
                                input_type,
                                e
                            ),
                        ))
                    },
                )?;

                let parsed_type = input.input_type();
                if parsed_type != input_type && input_type != "unknown" {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Input #{} type mismatch in input group: expected '{}', but got '{}'",
                            idx + 1,
                            input_type,
                            parsed_type
                        ),
                    )) as Box<dyn std::error::Error + Send>);
                }

                inputs.push(input);
            }
            return Ok(InputGroup::new(inputs));
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid input group format: expected an array of inputs or an object with 'inputs' field",
        )) as Box<dyn std::error::Error + Send>)
    }
}

// ============================================================================
// Processor Configuration
// ============================================================================

/// ProcessorConfig - Processor configuration
///
/// Contains basic processor information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    /// Processor name
    pub name: String,

    /// Input selector strategy when multiple inputs are available.
    /// Supported values: "round-robin" (default), "sequential", "priority", "group-parallel"
    #[serde(default = "default_input_selector")]
    pub input_selector: String,

    /// Whether to use built-in Event serialization
    ///
    /// If true, uses the system's built-in Event serialization method.
    /// If false or not set, uses default serialization method.
    #[serde(default)]
    pub use_builtin_event_serialization: bool,
    /// Whether to enable CheckPoint
    ///
    /// If true, enables checkpoint functionality.
    /// If false or not set, checkpoints are disabled.
    #[serde(default)]
    pub enable_checkpoint: bool,
    /// CheckPoint time interval (seconds)
    ///
    /// Time interval for checkpoints, minimum value is 1 second.
    /// If not set or less than 1, uses default value of 1 second.
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_seconds: u64,
    /// WASM initialization configuration (optional)
    ///
    /// Configuration parameters passed to wasm module's fs_init function.
    /// If not configured, an empty Map is passed.
    #[serde(default)]
    pub init_config: HashMap<String, String>,
    #[serde(default)]
    pub runtime: ProcessorRuntimeConfig,
}

/// Default checkpoint interval (1 second)
fn default_checkpoint_interval() -> u64 {
    1
}

/// Default input selector: round-robin
fn default_input_selector() -> String {
    "round-robin".to_string()
}

impl ProcessorConfig {
    /// Parse ProcessorConfig from YAML Value
    ///
    /// # Arguments
    /// - `value`: YAML Value object (root level config containing name, input-groups, outputs, etc.)
    ///
    /// # Returns
    /// - `Ok(ProcessorConfig)`: Successfully parsed
    /// - `Err(...)`: Parsing failed
    ///
    /// Note: Extracts processor-related fields from root level config, ignoring `type`, `input-groups`, `outputs`, etc.
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // Create a new Value containing only processor-related fields, excluding type, input-groups, outputs, etc.
        let mut processor_value = serde_yaml::Mapping::new();

        // Copy name field (if exists)
        if let Some(name_val) = value.get("name") {
            processor_value.insert(
                serde_yaml::Value::String("name".to_string()),
                name_val.clone(),
            );
        }

        // Copy input_selector / input-selector (if exists)
        if let Some(sel) = value
            .get("input_selector")
            .or_else(|| value.get("input-selector"))
        {
            processor_value.insert(
                serde_yaml::Value::String("input_selector".to_string()),
                sel.clone(),
            );
        }

        // Copy other processor-related fields (if exist)
        if let Some(use_builtin) = value.get("use_builtin_event_serialization") {
            processor_value.insert(
                serde_yaml::Value::String("use_builtin_event_serialization".to_string()),
                use_builtin.clone(),
            );
        }
        if let Some(enable_checkpoint) = value.get("enable_checkpoint") {
            processor_value.insert(
                serde_yaml::Value::String("enable_checkpoint".to_string()),
                enable_checkpoint.clone(),
            );
        }
        if let Some(checkpoint_interval) = value.get("checkpoint_interval_seconds") {
            processor_value.insert(
                serde_yaml::Value::String("checkpoint_interval_seconds".to_string()),
                checkpoint_interval.clone(),
            );
        }

        if let Some(init_config_val) = value.get("init_config") {
            processor_value.insert(
                serde_yaml::Value::String("init_config".to_string()),
                init_config_val.clone(),
            );
        }

        if let Some(runtime_val) = value.get("processor-runtime") {
            processor_value.insert(
                serde_yaml::Value::String("runtime".to_string()),
                runtime_val.clone(),
            );
        } else {
            let mut runtime_map = serde_yaml::Mapping::new();
            for key in &[
                "max-batch-size",
                "max-idle-count",
                "idle-sleep-ms",
                "control-timeout-ms",
                "control-max-retries",
            ] {
                if let Some(v) = value.get(*key) {
                    runtime_map.insert(serde_yaml::Value::String((*key).to_string()), v.clone());
                }
            }
            if !runtime_map.is_empty() {
                processor_value.insert(
                    serde_yaml::Value::String("runtime".to_string()),
                    serde_yaml::Value::Mapping(runtime_map),
                );
            }
        }

        // Parse ProcessorConfig from cleaned Value
        let clean_value = serde_yaml::Value::Mapping(processor_value);
        let mut config: ProcessorConfig = serde_yaml::from_value(clean_value).map_err(
            |e| -> Box<dyn std::error::Error + Send> {
                let available_keys: Vec<String> = value
                    .as_mapping()
                    .map(|m| {
                        m.keys()
                            .filter_map(|k| k.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Failed to parse processor config: {}. Available keys in root: {:?}",
                        e, available_keys
                    ),
                ))
            },
        )?;

        // If name is not provided, use default value
        if config.name.is_empty() {
            config.name = "default-processor".to_string();
        }

        // Normalize and validate input_selector
        let selector = config.input_selector.to_lowercase().trim().to_string();
        if selector.is_empty()
            || (!matches!(
                selector.as_str(),
                "round-robin" | "sequential" | "priority" | "group-parallel"
            ))
        {
            config.input_selector = default_input_selector();
        } else {
            config.input_selector = selector;
        }

        // Validate and fix checkpoint_interval_seconds (minimum value is 1 second)
        if config.checkpoint_interval_seconds < 1 {
            config.checkpoint_interval_seconds = 1;
        }

        Ok(config)
    }
}

// ============================================================================
// Output Configuration
// ============================================================================

/// OutputConfig - Output configuration
///
/// Supports multiple output types, each with its corresponding configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "output-type", rename_all = "kebab-case")]
pub enum OutputConfig {
    Kafka {
        bootstrap_servers: String,
        topic: String,
        partition: u32,
        #[serde(flatten)]
        extra: HashMap<String, String>,
        #[serde(default)]
        runtime: OutputRuntimeConfig,
    },
}

impl OutputConfig {
    /// Parse OutputConfig from YAML Value
    ///
    /// # Arguments
    /// - `value`: YAML Value object
    ///
    /// # Returns
    /// - `Ok(OutputConfig)`: Successfully parsed
    /// - `Err(...)`: Parsing failed
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        let config: OutputConfig = serde_yaml::from_value(value.clone()).map_err(
            |e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse output config: {}", e),
                ))
            },
        )?;
        Ok(config)
    }

    /// Get output type name
    pub fn output_type(&self) -> &'static str {
        match self {
            OutputConfig::Kafka { .. } => "kafka",
        }
    }

    pub fn output_runtime_config(&self) -> OutputRuntimeConfig {
        match self {
            OutputConfig::Kafka { runtime, .. } => runtime.clone(),
        }
    }
}

// ============================================================================
// WasmTask Configuration
// ============================================================================

/// WasmTaskConfig - WASM task configuration
///
/// Contains configuration for Input, Processor, and Output components.
#[derive(Debug, Clone)]
pub struct WasmTaskConfig {
    /// Task name
    pub task_name: String,
    /// Input group configuration list
    ///
    /// This is an array that can contain multiple input groups.
    /// Each input group contains multiple input source configurations,
    /// supporting reading from multiple data sources simultaneously.
    /// For example: can have multiple input groups, each containing inputs from multiple Kafka topics.
    pub input_groups: Vec<InputGroup>,
    /// Processor configuration
    pub processor: ProcessorConfig,
    /// Output configuration list
    ///
    /// This is an array that can contain multiple output configurations.
    /// Each output configuration represents a set of outputs,
    /// supporting writing to multiple data sources simultaneously.
    /// For example: can write to multiple Kafka topics simultaneously.
    pub outputs: Vec<OutputConfig>,
}

impl WasmTaskConfig {
    /// Parse WasmTaskConfig from YAML Value
    ///
    /// # Arguments
    /// - `task_name`: Task name (used if config doesn't have a name field)
    /// - `value`: YAML Value object (root level config containing name, type, input-groups, outputs, etc.)
    ///
    /// # Returns
    /// - `Ok(WasmTaskConfig)`: Successfully parsed
    /// - `Err(...)`: Parsing failed
    ///
    /// Configuration format:
    /// ```yaml
    /// name: "my-task"
    /// type: processor
    /// input-groups: [...]
    /// outputs: [...]
    /// ```
    pub fn from_yaml_value(
        task_name: String,
        value: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        use crate::runtime::wasm::task::yaml_keys::{INPUT_GROUPS, INPUTS, NAME, OUTPUTS};

        // 1. Get name from config (if exists), otherwise use the passed task_name
        let config_name = value
            .get(NAME)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(task_name);

        // 2. Parse Processor config (extract processor-related fields from root level config)
        // Note: Config is at root level, ProcessorConfig needs to parse from root level
        let mut processor = ProcessorConfig::from_yaml_value(value)?;

        // If ProcessorConfig name is empty, use config name
        if processor.name.is_empty() {
            processor.name = config_name.clone();
        }

        // 3. Parse Input Groups config
        // Supports two formats:
        // 1. Direct inputs array (backward compatible, parsed as single input group)
        // 2. input-groups array, each element is an input group
        let input_groups_value = value
            .get(INPUT_GROUPS)
            .or_else(|| value.get(INPUTS)) // Backward compatible
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Missing '{}' or '{}' in processor config",
                        INPUT_GROUPS, INPUTS
                    ),
                )) as Box<dyn std::error::Error + Send>
            })?;

        let input_groups_seq = input_groups_value.as_sequence().ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid 'input-groups' or 'inputs' format, expected a list",
            )) as Box<dyn std::error::Error + Send>
        })?;

        if input_groups_seq.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty 'input-groups' or 'inputs' list in config",
            )) as Box<dyn std::error::Error + Send>);
        }

        const MAX_INPUT_GROUPS: usize = 64;
        if input_groups_seq.len() > MAX_INPUT_GROUPS {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Too many input groups: {} (maximum is {})",
                    input_groups_seq.len(),
                    MAX_INPUT_GROUPS
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Parse each input group config
        // input-groups is an array, each element represents an input group (containing multiple input sources)
        let mut input_groups = Vec::new();
        for (group_idx, group_value) in input_groups_seq.iter().enumerate() {
            let input_group = InputGroup::from_yaml_value(group_value).map_err(
                |e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse input group #{}: {}", group_idx + 1, e),
                    ))
                },
            )?;

            // Validate input group is not empty
            if input_group.inputs.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Input group #{} is empty", group_idx + 1),
                )) as Box<dyn std::error::Error + Send>);
            }

            input_groups.push(input_group);
        }

        // 4. Parse Outputs config
        let outputs_value = value.get(OUTPUTS).ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing 'outputs' in processor config",
            )) as Box<dyn std::error::Error + Send>
        })?;

        let outputs_seq = outputs_value.as_sequence().ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid 'outputs' format, expected a list",
            )) as Box<dyn std::error::Error + Send>
        })?;

        if outputs_seq.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty 'outputs' list in config",
            )) as Box<dyn std::error::Error + Send>);
        }

        const MAX_OUTPUTS: usize = 64;
        if outputs_seq.len() > MAX_OUTPUTS {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Too many outputs: {} (maximum is {})",
                    outputs_seq.len(),
                    MAX_OUTPUTS
                ),
            )) as Box<dyn std::error::Error + Send>);
        }

        // Parse each output config
        // outputs is an array, each element represents an output configuration
        let mut outputs = Vec::new();
        for (idx, output_value) in outputs_seq.iter().enumerate() {
            // Try to get output type for clearer error message
            let output_type = output_value
                .get("output-type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let output = OutputConfig::from_yaml_value(output_value).map_err(
                |e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Failed to parse output #{} (type: {}): {}",
                            idx + 1,
                            output_type,
                            e
                        ),
                    ))
                },
            )?;

            // Validate output type matches
            let parsed_type = output.output_type();
            if parsed_type != output_type && output_type != "unknown" {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Output #{} type mismatch: expected '{}', but got '{}'",
                        idx + 1,
                        output_type,
                        parsed_type
                    ),
                )) as Box<dyn std::error::Error + Send>);
            }

            outputs.push(output);
        }

        Ok(WasmTaskConfig {
            task_name: config_name,
            input_groups,
            processor,
            outputs,
        })
    }
}
