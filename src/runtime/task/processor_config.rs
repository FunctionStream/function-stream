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

// Task Configuration - 任务配置结构体
//
// 定义 Input、Processor、Output 三个组件的配置结构体

use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;

// ============================================================================
// YAML Configuration Examples (YAML 配置示例)
// ============================================================================

/// YAML 配置示例
///
/// 包含各种配置类型的完整 YAML 示例，用于文档和解析参考
pub mod yaml_examples {
    /// Processor 类型配置的完整 YAML 示例
    ///
    /// 这是一个完整的 Processor 配置文件示例，包含：
    /// - name: 任务名称（根级别）
    /// - type: 配置类型（根级别，值为 "processor"）
    /// - input-groups: 输入组配置（支持多个输入组，每个组包含多个输入源）
    /// - outputs: 输出配置（支持多个输出接收器）
    ///
    /// 注意：配置文件在根级别包含 `name` 和 `type` 字段，用于区分配置类型。
    ///
    /// # 示例
    ///
    /// ```yaml
    /// name: "my-task"
    /// type: processor
    ///
    /// # input-groups 是一个数组，可以包含多个输入组
    /// # 每个输入组包含多个输入源配置，支持同时从多个数据源读取数据
    /// input-groups:
    ///   - inputs:
    ///       - input-type: kafka
    ///         bootstrap_servers: "localhost:9092"
    ///         topic: "input-topic-1"
    ///         partition: 0
    ///         group_id: "my-group"
    ///       - input-type: kafka
    ///         bootstrap_servers: "localhost:9092"
    ///         topic: "input-topic-2"
    ///         partition: 0
    ///         group_id: "my-group"
    ///
    /// # outputs 是一个数组，可以包含多个输出接收器配置
    /// # 支持同时向多个数据源写入数据
    /// outputs:
    ///   - output-type: kafka
    ///     bootstrap_servers: "localhost:9092"
    ///     topic: "output-topic"
    ///     partition: 0
    /// ```
    pub const PROCESSOR_CONFIG_EXAMPLE: &str = r#"name: "my-task"
type: processor

# input-groups 是一个数组，可以包含多个输入组
# 每个输入组包含多个输入源配置，支持同时从多个数据源读取数据
input-groups:
  - inputs:
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic-1"
        partition: 0
        group_id: "my-group"
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic-2"
        partition: 0
        group_id: "my-group"

# outputs 是一个数组，可以包含多个输出接收器配置
# 支持同时向多个数据源写入数据
outputs:
  - output-type: kafka
    bootstrap_servers: "localhost:9092"
    topic: "output-topic"
    partition: 0"#;

    /// Source 类型配置的完整 YAML 示例（未来支持）
    ///
    /// 这是一个 Source 配置示例，用于未来扩展。
    ///
    /// # 示例
    ///
    /// ```yaml
    /// name: "my-source"
    /// type: source
    /// # Source 配置内容（待实现）
    /// ```
    pub const SOURCE_CONFIG_EXAMPLE: &str = r#"name: "my-source"
type: source
# Source 配置内容（待实现）"#;

    /// Sink 类型配置的完整 YAML 示例（未来支持）
    ///
    /// 这是一个 Sink 配置示例，用于未来扩展。
    ///
    /// # 示例
    ///
    /// ```yaml
    /// name: "my-sink"
    /// type: sink
    /// # Sink 配置内容（待实现）
    /// ```
    pub const SINK_CONFIG_EXAMPLE: &str = r#"name: "my-sink"
type: sink
# Sink 配置内容（待实现）"#;
}

// ============================================================================
// Input Configuration (输入源配置)
// ============================================================================

/// InputConfig - 输入源配置
///
/// 支持多种输入源类型，每种类型有对应的配置结构
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "input-type", rename_all = "kebab-case")]
pub enum InputConfig {
    /// Kafka 输入源配置
    Kafka {
        /// Kafka 服务器地址
        bootstrap_servers: String,
        /// 主题名称
        topic: String,
        /// 分区编号（可选，不指定时使用 subscribe 自动分配）
        #[serde(default)]
        partition: Option<u32>,
        /// 消费者组 ID
        group_id: String,
        /// 其他 Kafka 配置项（可选）
        #[serde(flatten)]
        extra: HashMap<String, String>,
    },
}

impl InputConfig {
    /// 从 YAML Value 解析 InputConfig
    ///
    /// # 参数
    /// - `value`: YAML Value 对象
    ///
    /// # 返回值
    /// - `Ok(InputConfig)`: 成功解析
    /// - `Err(...)`: 解析失败
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // 先尝试使用 serde 反序列化
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

    /// 获取输入源类型名称
    pub fn input_type(&self) -> &'static str {
        match self {
            InputConfig::Kafka { .. } => "kafka",
        }
    }
}

// ============================================================================
// Input Group Configuration (输入组配置)
// ============================================================================

/// InputGroup - 输入组配置
///
/// 一个输入组包含多个输入源配置。
/// WASM 任务可以包含多个输入组，每个组可以包含多个输入源。
#[derive(Debug, Clone)]
pub struct InputGroup {
    /// 输入源配置列表
    ///
    /// 一个输入组可以包含多个输入源，这些输入源会被组合在一起处理。
    pub inputs: Vec<InputConfig>,
}

impl InputGroup {
    /// 创建新的输入组
    ///
    /// # 参数
    /// - `inputs`: 输入源配置列表
    ///
    /// # 返回值
    /// - `InputGroup`: 创建的输入组
    pub fn new(inputs: Vec<InputConfig>) -> Self {
        Self { inputs }
    }

    /// 从 YAML Value 解析 InputGroup
    ///
    /// # 参数
    /// - `value`: YAML Value 对象（应该是一个包含 inputs 数组的对象）
    ///
    /// # 返回值
    /// - `Ok(InputGroup)`: 成功解析
    /// - `Err(...)`: 解析失败
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // 如果 value 本身就是一个数组，直接解析为输入源列表
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

        // 如果 value 是一个对象，尝试获取 inputs 字段
        if let Some(inputs_value) = value.get("inputs")
            && let Some(inputs_seq) = inputs_value.as_sequence() {
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
                        ))
                            as Box<dyn std::error::Error + Send>);
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
// Processor Configuration (处理器配置)
// ============================================================================

/// ProcessorConfig - 处理器配置
///
/// 包含处理器的基本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    /// 处理器名称
    pub name: String,
    /// WASM 模块字节（不在配置文件中，运行时设置）
    ///
    /// 注意：此字段不会从配置文件序列化/反序列化，保证配置文件和 WASM 模块字节解耦
    #[serde(skip)]
    pub wasm_bytes: Option<Vec<u8>>,
    /// 是否使用内置 Event 序列化方式
    ///
    /// 如果为 true，则使用系统内置的 Event 序列化方式
    /// 如果为 false 或不设置，则使用默认序列化方式
    #[serde(default)]
    pub use_builtin_event_serialization: bool,
    /// 是否启用 CheckPoint
    ///
    /// 如果为 true，则启用检查点功能
    /// 如果为 false 或不设置，则不启用检查点
    #[serde(default)]
    pub enable_checkpoint: bool,
    /// CheckPoint 时间间隔（秒）
    ///
    /// 检查点的时间间隔，最小值为 1 秒
    /// 如果未设置或小于 1，则使用默认值 1 秒
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_seconds: u64,
    /// WASM 初始化配置（可选）
    ///
    /// 传递给 WASM 模块 fs_init 函数的配置参数
    /// 如果不配置，则传递空 Map
    #[serde(default)]
    pub init_config: HashMap<String, String>,
}

/// 默认检查点间隔（1 秒）
fn default_checkpoint_interval() -> u64 {
    1
}

impl ProcessorConfig {
    /// 从 YAML Value 解析 ProcessorConfig
    ///
    /// # 参数
    /// - `value`: YAML Value 对象（根级别的配置，包含 name、input-groups、outputs 等字段）
    ///
    /// # 返回值
    /// - `Ok(ProcessorConfig)`: 成功解析
    /// - `Err(...)`: 解析失败
    ///
    /// 注意：需要从根级别配置中提取 processor 相关字段，忽略 `type`、`input-groups`、`outputs` 等字段。
    pub fn from_yaml_value(value: &Value) -> Result<Self, Box<dyn std::error::Error + Send>> {
        // 创建一个新的 Value，只包含 processor 相关字段，排除 type、input-groups、outputs 等
        let mut processor_value = serde_yaml::Mapping::new();

        // 复制 name 字段（如果存在）
        if let Some(name_val) = value.get("name") {
            processor_value.insert(
                serde_yaml::Value::String("name".to_string()),
                name_val.clone(),
            );
        }

        // 复制其他 processor 相关字段（如果存在）
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

        // 从清理后的 Value 解析 ProcessorConfig
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

        // 如果没有提供 name，使用默认值
        if config.name.is_empty() {
            config.name = "default-processor".to_string();
        }

        // 验证并修正 checkpoint_interval_seconds（最小值为 1 秒）
        if config.checkpoint_interval_seconds < 1 {
            config.checkpoint_interval_seconds = 1;
        }

        Ok(config)
    }

    /// 设置 WASM 模块字节
    ///
    /// # 参数
    /// - `bytes`: WASM 模块字节
    pub fn set_wasm_bytes(&mut self, bytes: Vec<u8>) {
        self.wasm_bytes = Some(bytes);
    }
}

// ============================================================================
// Output Configuration (输出配置)
// ============================================================================

/// OutputConfig - 输出配置
///
/// 支持多种输出类型，每种类型有对应的配置结构
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "output-type", rename_all = "kebab-case")]
pub enum OutputConfig {
    /// Kafka 输出接收器配置
    Kafka {
        /// Kafka 服务器地址
        bootstrap_servers: String,
        /// 主题名称
        topic: String,
        /// 分区编号
        partition: u32,
        /// 其他 Kafka 配置项（可选）
        #[serde(flatten)]
        extra: HashMap<String, String>,
    },
}

impl OutputConfig {
    /// 从 YAML Value 解析 OutputConfig
    ///
    /// # 参数
    /// - `value`: YAML Value 对象
    ///
    /// # 返回值
    /// - `Ok(OutputConfig)`: 成功解析
    /// - `Err(...)`: 解析失败
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

    /// 获取输出类型名称
    pub fn output_type(&self) -> &'static str {
        match self {
            OutputConfig::Kafka { .. } => "kafka",
        }
    }
}

// ============================================================================
// WasmTask Configuration (WASM 任务配置)
// ============================================================================

/// WasmTaskConfig - WASM 任务配置
///
/// 包含 Input、Processor、Output 三个组件的配置
#[derive(Debug, Clone)]
pub struct WasmTaskConfig {
    /// 任务名称
    pub task_name: String,
    /// 输入组配置列表
    ///
    /// 这是一个数组，可以包含多个输入组。
    /// 每个输入组包含多个输入源配置，支持同时从多个数据源读取数据。
    /// 例如：可以有多个输入组，每个组包含多个 Kafka topic 的输入源。
    pub input_groups: Vec<InputGroup>,
    /// 处理器配置
    pub processor: ProcessorConfig,
    /// 输出配置列表
    ///
    /// 这是一个数组，可以包含多个输出配置。
    /// 每个输出配置代表一组输出，支持同时向多个数据源写入数据。
    /// 例如：可以同时向多个 Kafka topic 写入数据。
    pub outputs: Vec<OutputConfig>,
}

impl WasmTaskConfig {
    /// 从 YAML Value 解析 WasmTaskConfig
    ///
    /// # 参数
    /// - `task_name`: 任务名称（如果配置中没有 name 字段，则使用此值）
    /// - `value`: YAML Value 对象（根级别的配置，包含 name、type、input-groups、outputs 等）
    ///
    /// # 返回值
    /// - `Ok(WasmTaskConfig)`: 成功解析
    /// - `Err(...)`: 解析失败
    ///
    /// 配置格式：
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
        use crate::runtime::task::yaml_keys::{INPUT_GROUPS, INPUTS, NAME, OUTPUTS};

        // 1. 获取配置中的 name（如果存在），否则使用传入的 task_name
        let config_name = value
            .get(NAME)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(task_name);

        // 2. 解析 Processor 配置（从根级别配置中提取 processor 相关字段）
        // 注意：现在配置在根级别，ProcessorConfig 需要能够从根级别解析
        let mut processor = ProcessorConfig::from_yaml_value(value)?;

        // 如果 ProcessorConfig 中的 name 为空，使用配置中的 name
        if processor.name.is_empty() {
            processor.name = config_name.clone();
        }

        // 3. 解析 Input Groups 配置
        // 支持两种格式：
        // 1. 直接是 inputs 数组（向后兼容，会被解析为单个输入组）
        // 2. input-groups 数组，每个元素是一个输入组
        let input_groups_value = value
            .get(INPUT_GROUPS)
            .or_else(|| value.get(INPUTS)) // 向后兼容
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

        // 解析每个输入组配置
        // input-groups 是一个数组，每个元素代表一个输入组（包含多个输入源）
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

            // 验证输入组不为空
            if input_group.inputs.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Input group #{} is empty", group_idx + 1),
                )) as Box<dyn std::error::Error + Send>);
            }

            input_groups.push(input_group);
        }

        // 4. 解析 Outputs 配置
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

        // 解析每个输出配置
        // outputs 是一个数组，每个元素代表一组输出配置
        let mut outputs = Vec::new();
        for (idx, output_value) in outputs_seq.iter().enumerate() {
            // 尝试获取输出类型，用于更清晰的错误消息
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

            // 验证输出类型是否匹配
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
