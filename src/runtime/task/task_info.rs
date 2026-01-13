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

// TaskInfo - Task information
//
// Defines basic task information and configuration

use std::collections::HashMap;

/// Configuration type
///
/// Defines different types of configuration files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigType {
    /// Processor Configuration type
    ///
    /// Contains processor, input-groups, outputs and other configurations
    Processor,
    /// Source configuration type (future support)
    ///
    /// Contains input source related configurations
    Source,
    /// Sink configuration type (future support)
    ///
    /// Contains output sink related configurations
    Sink,
}

impl ConfigType {
    /// Get string representation of configuration type
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigType::Processor => "processor",
            ConfigType::Source => "source",
            ConfigType::Sink => "sink",
        }
    }

    /// Create configuration type from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "processor" => Some(ConfigType::Processor),
            "source" => Some(ConfigType::Source),
            "sink" => Some(ConfigType::Sink),
            _ => None,
        }
    }
}

/// TaskInfo configuration key name constants
///
/// Defines all configuration key names used in TaskInfo.config
pub mod config_keys {
    use super::ConfigType;

    /// Processor configuration file path key name
    ///
    /// Stores path to Processor type configuration file (YAML format).
    /// System will read configuration file from this path.
    ///
    /// For configuration format examples, refer to `processor_config::yaml_examples::PROCESSOR_CONFIG_EXAMPLE`
    pub const PROCESSOR_CONFIG_PATH: &str = "processor_config_path";

    /// Source configuration file path key name (future support)
    ///
    /// Stores path to Source type configuration file (YAML format).
    /// System will read configuration file from this path.
    pub const SOURCE_CONFIG_PATH: &str = "source_config_path";

    /// Sink configuration file path key name (future support)
    ///
    /// Stores path to Sink type configuration file (YAML format).
    /// System will read configuration file from this path.
    pub const SINK_CONFIG_PATH: &str = "sink_config_path";

    /// WASM module path key name
    ///
    /// Stores path to WASM module file.
    /// This is an optional field, if not provided, empty string is used.
    ///
    /// Note: wasm_path is not in configuration file, ensuring decoupling between configuration file and WASM module path.
    pub const WASM_PATH: &str = "wasm_path";

    /// Get corresponding configuration path key name based on configuration type
    ///
    /// # Arguments
    /// - `config_type`: Configuration type
    ///
    /// # Returns
    /// - `&str`: Corresponding configuration path key name
    pub fn get_config_path_key(config_type: ConfigType) -> &'static str {
        match config_type {
            ConfigType::Processor => PROCESSOR_CONFIG_PATH,
            ConfigType::Source => SOURCE_CONFIG_PATH,
            ConfigType::Sink => SINK_CONFIG_PATH,
        }
    }
}

/// Task information
///
/// Contains basic task information and configuration data.
///
/// # Configuration key names
///
/// `config` field is a `HashMap<String, String>`，Supports the following key names:
///
/// - `config_keys::PROCESSOR_CONFIG_PATH` ("processor_config_path"): Processor configuration file path (required)
///   - Specify path to Processor type configuration file (YAML format)
///   - System will read configuration file from this path
///   - Contains processor, input-groups, outputs and other configurations
///
/// - `config_keys::SOURCE_CONFIG_PATH` ("source_config_path"): Source configuration file path (optional, future support)
///   - Specify path to Source type configuration file (YAML format)
///   - System will read configuration file from this path
///
/// - `config_keys::SINK_CONFIG_PATH` ("sink_config_path"): Sink configuration file path (optional, future support)
///   - Specify path to Sink type configuration file (YAML format)
///   - System will read configuration file from this path
///
/// - `config_keys::WASM_PATH` ("wasm_path"): WASM module path (optional)
///   - Specify path to WASM module file
///   - If not provided, empty string will be used
///
/// # Examples
///
/// ```rust
/// use std::collections::HashMap;
/// use crate::runtime::task::TaskInfo;
/// use crate::runtime::task::task_info::config_keys;
///
/// let mut config = HashMap::new();
/// config.insert(config_keys::PROCESSOR_CONFIG_PATH.to_string(), "/path/to/processor_config.yaml".to_string());
/// config.insert(config_keys::WASM_PATH.to_string(), "/path/to/module.wasm".to_string());
///
/// let task_info = TaskInfo::with_config("my-task".to_string(), config);
/// ```
#[derive(Clone, Debug)]
pub struct TaskInfo {
    pub task_name: String,
    /// Configuration information (key-value pairs)
    ///
    /// Supported key names:
    /// - `config_keys::PROCESSOR_CONFIG_PATH`: Processor configuration file path (required)
    /// - `config_keys::SOURCE_CONFIG_PATH`: Source configuration file path (optional, future support)
    /// - `config_keys::SINK_CONFIG_PATH`: Sink configuration file path (optional, future support)
    /// - `config_keys::WASM_PATH`: WASM module path (optional)
    pub config: HashMap<String, String>,
}

impl TaskInfo {
    /// Create new task information (without configuration)
    pub fn new(task_name: String) -> Self {
        Self {
            task_name,
            config: HashMap::new(),
        }
    }

    /// Create task information with configuration
    pub fn with_config(task_name: String, config: HashMap<String, String>) -> Self {
        Self { task_name, config }
    }
}
