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

// Task Storage Factory
//
// Provides factory methods for creating task storage instances based on configuration

use super::rocksdb_storage::RocksDBTaskStorage;
use super::storage::TaskStorage;
use crate::config::find_or_create_data_dir;
use crate::config::storage::{TaskStorageConfig, TaskStorageType};
use anyhow::{Context, Result};
use std::path::Path;

/// Task storage factory
pub struct TaskStorageFactory;

impl TaskStorageFactory {
    /// Create a task storage instance based on configuration
    ///
    /// # Arguments
    /// - `config`: Task storage configuration
    ///
    /// # Returns
    /// - `Ok(Box<dyn TaskStorage>)`: Successfully created storage instance
    /// - `Err(...)`: Creation failed
    pub fn create_storage(
        config: &TaskStorageConfig,
    ) -> Result<Box<dyn TaskStorage>> {
        match config.storage_type {
            TaskStorageType::RocksDB => {
                // Determine database path
                let db_path = if let Some(ref path) = config.db_path {
                    // Use the path specified in configuration
                    Path::new(path).to_path_buf()
                } else {
                    // Use default path: data/task
                    let data_dir = find_or_create_data_dir()
                        .context("Failed to find or create data directory")?;
                    data_dir.join("task")
                };

                // Ensure directory exists
                if let Some(parent) = db_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context(format!("Failed to create directory: {:?}", parent))?;
                }

                // Create RocksDB storage instance
                let storage = RocksDBTaskStorage::new(db_path, Some(&config.rocksdb))?;
                Ok(Box::new(storage))
            }
        }
    }
}
