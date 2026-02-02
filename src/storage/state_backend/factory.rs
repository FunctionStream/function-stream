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

use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::store::StateStore;
use std::path::Path;
use std::sync::Arc;

/// State store factory interface
///
/// All state store factories should implement this interface
pub trait StateStoreFactory: Send + Sync {
    /// Create a new state store instance
    ///
    /// # Arguments
    /// - `column_family`: optional column family name (some implementations may not support)
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateStore>)`: successfully created
    /// - `Err(BackendError)`: creation failed
    fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn StateStore>, BackendError>;
}

/// Factory type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryType {
    /// Memory factory
    Memory,
    /// RocksDB factory
    RocksDB,
}

pub fn get_factory_for_task<P: AsRef<Path>>(
    factory_type: FactoryType,
    task_name: String,
    created_at: u64,
    base_dir: Option<P>,
    rocksdb_config: Option<crate::storage::state_backend::rocksdb::RocksDBConfig>,
) -> Result<Arc<dyn StateStoreFactory>, BackendError> {
    match factory_type {
        FactoryType::Memory => {
            Ok(crate::storage::state_backend::memory::MemoryStateStoreFactory::default_factory())
        }
        FactoryType::RocksDB => {
            let base_dir = base_dir.ok_or_else(|| {
                BackendError::Other("base_dir is required for RocksDB factory".to_string())
            })?;

            let db_path = base_dir
                .as_ref()
                .join(format!("{}-{}", task_name, created_at));

            let config = rocksdb_config.unwrap_or_default();
            let factory = crate::storage::state_backend::rocksdb::RocksDBStateStoreFactory::new(
                db_path, config,
            )?;

            Ok(Arc::new(factory))
        }
    }
}
