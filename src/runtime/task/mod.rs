// Task module - Task lifecycle management

mod lifecycle;
mod task_info;
mod processor_config;
mod yaml_keys;
mod builder;

pub use lifecycle::*;
pub use task_info::{TaskInfo, ConfigType, config_keys};
pub use processor_config::{InputConfig, InputGroup, ProcessorConfig, OutputConfig, WasmTaskConfig, yaml_examples};
pub use yaml_keys::{TYPE, NAME, INPUT_GROUPS, INPUTS, OUTPUTS, type_values};
pub use builder::{TaskBuilder, ProcessorBuilder, SourceBuilder, SinkBuilder};

