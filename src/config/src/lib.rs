//! Configuration loading and validation.

pub mod global_config;
pub mod loader;
pub mod log_config;
pub mod paths;
pub mod python_config;
pub mod service_config;
pub mod storage;
pub mod streaming_job;
pub mod system;
pub mod wasm_config;

// Compatibility shim for files that still reference `crate::config::*`.
pub mod config {
    pub use crate::*;
}

pub use global_config::{
    DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES, DEFAULT_STREAMING_RUNTIME_MEMORY_BYTES, GlobalConfig,
};
pub use loader::load_global_config;
pub use log_config::LogConfig;
#[allow(unused_imports)]
pub use paths::{
    ENV_CONF, ENV_HOME, find_config_file, get_app_log_path, get_conf_dir, get_data_dir,
    get_log_path, get_logs_dir, get_project_root, get_python_cache_dir, get_python_cwasm_path,
    get_python_wasm_path, get_state_dir, get_state_dir_for_base, get_task_dir, get_wasm_cache_dir,
    resolve_path,
};
#[cfg(feature = "python")]
pub use python_config::PythonConfig;
pub use streaming_job::{DEFAULT_CHECKPOINT_INTERVAL_MS, DEFAULT_PIPELINE_PARALLELISM};
