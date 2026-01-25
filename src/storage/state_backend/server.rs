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

use crate::config::storage::StateStorageConfig;
use crate::storage::state_backend::STATE_DIR_NAME;
use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::factory::{
    FactoryType, StateStoreFactory, get_factory_for_task,
};
use std::sync::Arc;

/// State storage server
///
/// Creates state store factories based on configuration
pub struct StateStorageServer {
    /// State storage configuration
    config: StateStorageConfig,
}

impl StateStorageServer {
    /// Create a new state storage server
    ///
    /// # Arguments
    /// - `config`: state storage configuration
    ///
    /// # Returns
    /// - `Ok(StateStorageServer)`: successfully created
    /// - `Err(BackendError)`: creation failed
    pub fn new(config: StateStorageConfig) -> Result<Self, BackendError> {
        let factory_type = match config.storage_type {
            crate::config::storage::StateStorageType::Memory => FactoryType::Memory,
            crate::config::storage::StateStorageType::RocksDB => FactoryType::RocksDB,
        };

        if factory_type == FactoryType::RocksDB && config.base_dir.is_none() {
            return Err(BackendError::Other(
                "base_dir is required for RocksDB factory type".to_string(),
            ));
        }

        if factory_type == FactoryType::RocksDB
            && let Some(ref base_dir) = config.base_dir {
                let state_dir = std::path::Path::new(base_dir).join(STATE_DIR_NAME);
                std::fs::create_dir_all(&state_dir).map_err(|e| {
                    BackendError::IoError(format!("Failed to create state directory: {}", e))
                })?;
            }

        Ok(Self { config })
    }

    /// Create a state store factory
    ///
    /// # Arguments
    /// - `task_name`: task name
    /// - `created_at`: creation time (Unix timestamp in seconds)
    ///
    /// # Returns
    /// - `Ok(Arc<dyn StateStoreFactory>)`: factory created successfully
    /// - `Err(BackendError)`: creation failed
    pub fn create_factory(
        &self,
        task_name: String,
        created_at: u64,
    ) -> Result<Arc<dyn StateStoreFactory>, BackendError> {
        let factory_type = match self.config.storage_type {
            crate::config::storage::StateStorageType::Memory => FactoryType::Memory,
            crate::config::storage::StateStorageType::RocksDB => FactoryType::RocksDB,
        };

        let rocksdb_config = if factory_type == FactoryType::RocksDB {
            Some(
                crate::storage::state_backend::rocksdb::RocksDBConfig::from(
                    &self.config.rocksdb,
                ),
            )
        } else {
            None
        };

        let state_dir = if factory_type == FactoryType::RocksDB {
            self.config
                .base_dir
                .as_ref()
                .map(|base_dir| std::path::Path::new(base_dir).join(STATE_DIR_NAME))
        } else {
            None
        };

        get_factory_for_task(
            factory_type,
            task_name,
            created_at,
            state_dir.as_deref(),
            rocksdb_config,
        )
    }
}
