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
use crate::storage::state_backend::factory::StateStoreFactory;
use rocksdb::{DB, Options};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// RocksDB configuration options
#[derive(Debug, Clone, Default)]
pub struct RocksDBConfig {
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
}

/// Convert from configuration struct to RocksDBConfig
impl From<&crate::config::storage::RocksDBStorageConfig> for RocksDBConfig {
    fn from(config: &crate::config::storage::RocksDBStorageConfig) -> Self {
        Self {
            max_open_files: config.max_open_files,
            write_buffer_size: config.write_buffer_size,
            max_write_buffer_number: config.max_write_buffer_number,
            target_file_size_base: config.target_file_size_base,
            max_bytes_for_level_base: config.max_bytes_for_level_base,
        }
    }
}

/// RocksDB state store factory
pub struct RocksDBStateStoreFactory {
    /// RocksDB database instance
    db: Arc<DB>,
    /// Lock for protecting column family creation operations
    cf_creation_lock: Mutex<()>,
}

impl StateStoreFactory for RocksDBStateStoreFactory {
    fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn crate::storage::state_backend::store::StateStore>, BackendError> {
        self.new_state_store(column_family)
    }
}

impl RocksDBStateStoreFactory {
    /// Create a new RocksDB state store factory
    ///
    /// # Arguments
    /// - `db_path`: database path
    /// - `config`: RocksDB configuration
    ///
    /// # Returns
    /// - `Ok(RocksDBStateStoreFactory)`: successfully created
    /// - `Err(BackendError)`: creation failed
    pub fn new<P: AsRef<Path>>(db_path: P, config: RocksDBConfig) -> Result<Self, BackendError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        if let Some(max_open_files) = config.max_open_files {
            opts.set_max_open_files(max_open_files);
        }
        if let Some(write_buffer_size) = config.write_buffer_size {
            opts.set_write_buffer_size(write_buffer_size);
        }
        if let Some(max_write_buffer_number) = config.max_write_buffer_number {
            opts.set_max_write_buffer_number(max_write_buffer_number);
        }
        if let Some(target_file_size_base) = config.target_file_size_base {
            opts.set_target_file_size_base(target_file_size_base);
        }
        if let Some(max_bytes_for_level_base) = config.max_bytes_for_level_base {
            opts.set_max_bytes_for_level_base(max_bytes_for_level_base);
        }

        opts.set_merge_operator_associative("appendOp", merge_operator);

        let db_path = db_path.as_ref();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BackendError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        let db = DB::open(&opts, db_path)
            .map_err(|e| BackendError::IoError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self {
            db: Arc::new(db),
            cf_creation_lock: Mutex::new(()),
        })
    }

    /// Create a new state store instance
    ///
    /// # Arguments
    /// - `column_family`: optional column family name, if None uses default column family
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateStore>)`: successfully created
    /// - `Err(BackendError)`: creation failed
    ///
    /// Note: If a column family name is specified and it doesn't exist, it will be created automatically
    pub fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn crate::storage::state_backend::store::StateStore>, BackendError> {
        if let Some(ref cf_name) = column_family
            && cf_name != "default"
            && self.db.cf_handle(cf_name).is_none()
        {
            let _guard = self.cf_creation_lock.lock().map_err(|e| {
                BackendError::Other(format!("Failed to acquire cf creation lock: {}", e))
            })?;

            if self.db.cf_handle(cf_name).is_none() {
                log::info!("Creating column family '{}' as it does not exist", cf_name);
                let opts = Options::default();
                self.db.create_cf(cf_name, &opts).map_err(|e| {
                    BackendError::Other(format!(
                        "Failed to create column family '{}': {}",
                        cf_name, e
                    ))
                })?;
            }
        }

        crate::storage::state_backend::rocksdb::store::RocksDBStateStore::new_with_factory(
            self.db.clone(),
            column_family,
        )
    }
}

/// Merge operator: for merging values (append operation)
fn merge_operator(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    use std::io::Write;

    let mut buf = Vec::new();

    for operand in operands {
        buf.write_all(operand).ok()?;
    }

    if let Some(existing) = existing_val {
        buf.write_all(existing).ok()?;
    }

    Some(buf)
}
