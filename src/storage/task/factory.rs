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

use super::rocksdb_storage::RocksDBTaskStorage;
use super::storage::TaskStorage;
use crate::config::storage::{TaskStorageConfig, TaskStorageType};
use crate::config::{get_task_dir, resolve_path};
use anyhow::{Context, Result};

pub struct TaskStorageFactory;

impl TaskStorageFactory {
    pub fn create_storage(config: &TaskStorageConfig) -> Result<Box<dyn TaskStorage>> {
        match config.storage_type {
            TaskStorageType::RocksDB => {
                let db_path = if let Some(ref path) = config.db_path {
                    resolve_path(path)
                } else {
                    get_task_dir()
                };

                if let Some(parent) = db_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context(format!("Failed to create directory: {:?}", parent))?;
                }

                let storage = RocksDBTaskStorage::new(db_path, Some(&config.rocksdb))?;
                Ok(Box::new(storage))
            }
        }
    }
}
