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

// Storage Configuration - Storage configuration
//
// Defines configuration structures for state storage and task storage

use serde::{Deserialize, Serialize};

/// State storage factory type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateStorageType {
    /// Memory storage
    Memory,
    /// RocksDB storage
    RocksDB,
}

/// RocksDB configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct RocksDBStorageConfig {
    // Note: dir_name is no longer used, database is stored directly in {base_dir}/state/{task_name}-{time} directory
    // Example: data/state/my_task-1234567890
    /// Maximum number of open files
    pub max_open_files: Option<i32>,
    /// Write buffer size (bytes)
    pub write_buffer_size: Option<usize>,
    /// Maximum number of write buffers
    pub max_write_buffer_number: Option<i32>,
    /// Target file size base (bytes)
    pub target_file_size_base: Option<u64>,
    /// Maximum bytes for level base (bytes)
    pub max_bytes_for_level_base: Option<u64>,
    // Note: Compression configuration is not currently supported, uses default none compression
}


/// State storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateStorageConfig {
    /// Storage type
    #[serde(default = "default_state_storage_type")]
    pub storage_type: StateStorageType,
    /// Base directory path (required for RocksDB)
    /// Final path format: {base_dir}/state/{task_name}-{created_at}
    /// Example: if base_dir is "data", task name is "my_task", created_at is 1234567890
    /// then the full path is: data/state/my_task-1234567890
    /// Default uses the data directory returned by find_or_create_data_dir()
    #[serde(default = "default_base_dir")]
    pub base_dir: Option<String>,
    /// RocksDB configuration (only used when storage_type is RocksDB)
    #[serde(default)]
    pub rocksdb: RocksDBStorageConfig,
}

fn default_state_storage_type() -> StateStorageType {
    StateStorageType::RocksDB
}

fn default_base_dir() -> Option<String> {
    // Default base directory is "data" (lowercase)
    // In actual use, if not specified in config, should use the result of find_or_create_data_dir()
    Some("data".to_string())
}

impl Default for StateStorageConfig {
    fn default() -> Self {
        Self {
            storage_type: StateStorageType::RocksDB,
            base_dir: default_base_dir(),
            rocksdb: RocksDBStorageConfig::default(),
        }
    }
}

/// Task storage type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStorageType {
    /// RocksDB storage
    RocksDB,
}

/// Task storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStorageConfig {
    /// Storage type
    #[serde(default = "default_task_storage_type")]
    pub storage_type: TaskStorageType,
    /// Database path (optional, if None, uses default path `data/task/{task_name}`)
    /// Default path format: `data/task/{task_name}`
    /// Example: `data/task/my_task`
    pub db_path: Option<String>,
    /// RocksDB configuration
    #[serde(default)]
    pub rocksdb: RocksDBStorageConfig,
}

fn default_task_storage_type() -> TaskStorageType {
    TaskStorageType::RocksDB
}

impl Default for TaskStorageConfig {
    fn default() -> Self {
        Self {
            storage_type: TaskStorageType::RocksDB,
            db_path: None,
            rocksdb: RocksDBStorageConfig::default(),
        }
    }
}
