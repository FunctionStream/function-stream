use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use uuid::Uuid;

/// Service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// Service ID
    pub service_id: String,
    /// Service name
    pub service_name: String,
    /// Service version
    pub version: String,
    /// Host address
    pub host: String,
    /// Port
    pub port: u16,
    /// Number of worker threads (if specified, overrides worker_multiplier)
    pub workers: Option<usize>,
    /// Worker thread multiplier (CPU cores × multiplier)
    /// If not specified in config file, default value is 4
    pub worker_multiplier: Option<usize>,
    /// Debug mode
    pub debug: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Log level
    pub level: String,
    /// Log format
    pub format: String,
    /// Log file path
    pub file_path: Option<String>,
    /// Maximum file size (MB)
    pub max_file_size: Option<u64>,
    /// Number of files to retain
    pub max_files: Option<u32>,
}

/// Global configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// Service configuration
    pub service: ServiceConfig,
    /// Logging configuration
    pub logging: LogConfig,
    /// State storage configuration
    #[serde(default)]
    pub state_storage: crate::config::storage::StateStorageConfig,
    /// Task storage configuration
    #[serde(default)]
    pub task_storage: crate::config::storage::TaskStorageConfig,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            service: ServiceConfig::default(),
            logging: LogConfig::default(),
            state_storage: crate::config::storage::StateStorageConfig::default(),
            task_storage: crate::config::storage::TaskStorageConfig::default(),
        }
    }
}

impl GlobalConfig {
    /// Create configuration with version information from Cargo.toml
    pub fn from_cargo() -> Self {
        let mut config = Self::default();
        config.service.version = env!("CARGO_PKG_VERSION").to_string();
        config.service.service_name = Uuid::new_v4().to_string();
        config
    }

    /// Get version information from compile time
    pub fn cargo_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            service_id: "default-service".to_string(),
            service_name: "function-stream".to_string(),
            version: "0.1.0".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            workers: None,
            worker_multiplier: Some(4),
            debug: false,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
            file_path: Some("logs/app.log".to_string()),
            max_file_size: Some(100),
            max_files: Some(5),
        }
    }
}

impl GlobalConfig {
    /// Create new global configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create global configuration from YAML value
    pub fn from_yaml_value(value: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let config: GlobalConfig = serde_yaml::from_value(value)?;
        Ok(config)
    }

    /// Get service ID
    pub fn service_id(&self) -> &str {
        &self.service.service_id
    }

    /// Get service port
    pub fn port(&self) -> u16 {
        self.service.port
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate port range
        if self.service.port == 0 {
            return Err(format!("Invalid port: {}", self.service.port));
        }

        Ok(())
    }
}

impl GlobalConfig {
    /// Load configuration from file path, use default path if None
    pub fn load<P: AsRef<std::path::Path>>(path: Option<P>) -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = path
            .map(|p| p.as_ref().to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("config.yaml"));

        if config_path.exists() {
            crate::config::load_global_config(&config_path)
        } else {
            // If config file doesn't exist, use default configuration
            Ok(Self::default())
        }
    }
}

