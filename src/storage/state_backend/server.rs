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

use crate::config::storage::{StateStorageConfig, StateStorageType};
use crate::config::{get_state_dir, get_state_dir_for_base};
use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::factory::{
    FactoryType, StateStoreFactory, get_factory_for_task,
};
use crate::storage::state_backend::rocksdb::RocksDBConfig;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

pub struct StateStorageServer {
    config: Arc<StateStorageConfig>,
    resolved_state_dir: Option<PathBuf>,
    factory_type: FactoryType,
}

impl StateStorageServer {
    pub fn new(config: StateStorageConfig) -> Result<Self, BackendError> {
        let factory_type = match config.storage_type {
            StateStorageType::Memory => FactoryType::Memory,
            StateStorageType::RocksDB => FactoryType::RocksDB,
        };

        let resolved_state_dir = Self::resolve_and_prepare_dir(&config, &factory_type)?;

        Ok(Self {
            config: Arc::new(config),
            resolved_state_dir,
            factory_type,
        })
    }

    fn resolve_and_prepare_dir(
        config: &StateStorageConfig,
        factory_type: &FactoryType,
    ) -> Result<Option<PathBuf>, BackendError> {
        if !matches!(factory_type, FactoryType::RocksDB) {
            return Ok(None);
        }

        let base_dir = config.base_dir.as_deref().unwrap_or("data");

        let state_dir = if base_dir == "data" {
            get_state_dir()
        } else {
            get_state_dir_for_base(base_dir)
        };

        fs::create_dir_all(&state_dir).map_err(|e| {
            BackendError::IoError(format!(
                "Failed to create state storage directory at {:?}: {}",
                state_dir, e
            ))
        })?;

        let final_path = state_dir.canonicalize().unwrap_or(state_dir);
        Ok(Some(final_path))
    }

    pub fn create_factory(
        &self,
        task_name: String,
        created_at: u64,
    ) -> Result<Arc<dyn StateStoreFactory>, BackendError> {
        let rocksdb_config = if self.factory_type == FactoryType::RocksDB {
            Some(RocksDBConfig::from(&self.config.rocksdb))
        } else {
            None
        };

        get_factory_for_task(
            self.factory_type,
            task_name,
            created_at,
            self.resolved_state_dir.as_deref(),
            rocksdb_config,
        )
    }
}
