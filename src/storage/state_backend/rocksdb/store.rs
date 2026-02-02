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
use crate::storage::state_backend::key_builder::{build_key, increment_key, is_all_0xff};
use crate::storage::state_backend::store::{StateIterator, StateStore};
use rocksdb::{DB, IteratorMode, WriteBatch};
use std::sync::{Arc, Mutex};

/// RocksDB state store
pub struct RocksDBStateStore {
    /// RocksDB database instance
    db: Arc<DB>,
    /// Column family name (if using column family)
    ///
    /// In MultiThreaded mode, get handle by name for each operation
    /// This avoids lifetime and pointer issues
    column_family_name: Option<String>,
}

impl RocksDBStateStore {
    /// Create a new state store instance from factory
    ///
    /// # Arguments
    /// - `db`: database instance
    /// - `column_family`: optional column family name
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateStore>)`: successfully created
    /// - `Err(BackendError)`: creation failed
    ///
    /// Note: Caller should ensure the column family already exists (factory will create it automatically)
    pub fn new_with_factory(
        db: Arc<DB>,
        column_family: Option<String>,
    ) -> Result<Box<dyn StateStore>, BackendError> {
        let cf_name = if let Some(ref cf_name) = column_family {
            if cf_name != "default" {
                if db.cf_handle(cf_name).is_none() {
                    return Err(BackendError::Other(format!(
                        "Column family '{}' does not exist. This should not happen as factory should create it.",
                        cf_name
                    )));
                }
                Some(cf_name.clone())
            } else {
                None
            }
        } else {
            None
        };

        Ok(Box::new(Self {
            db,
            column_family_name: cf_name,
        }))
    }

    /// Open RocksDB store (static method, simplified version)
    ///
    /// # Arguments
    /// - `name`: database path
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateStore>)`: successfully opened
    /// - `Err(BackendError)`: open failed
    pub fn open<P: AsRef<std::path::Path>>(name: P) -> Result<Box<dyn StateStore>, BackendError> {
        let path = name.as_ref();

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("appendOp", merge_operator);

        let db = DB::open(&opts, path)
            .map_err(|e| BackendError::IoError(format!("Failed to open RocksDB: {}", e)))?;

        Self::new_with_factory(Arc::new(db), None)
    }

    /// Helper method for operations using column family
    fn put_cf(&self, key: &[u8], value: &[u8]) -> Result<(), BackendError> {
        if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.put_cf(&cf, key, value)
        } else {
            self.db.put(key, value)
        }
        .map_err(|e| BackendError::IoError(format!("RocksDB put error: {}", e)))?;
        Ok(())
    }

    fn get_cf(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BackendError> {
        let result = if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.get_cf(&cf, key)
        } else {
            self.db.get(key)
        }
        .map_err(|e| BackendError::IoError(format!("RocksDB get error: {}", e)))?;

        Ok(result)
    }

    fn delete_cf(&self, key: &[u8]) -> Result<(), BackendError> {
        if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.delete_cf(&cf, key)
        } else {
            self.db.delete(key)
        }
        .map_err(|e| BackendError::IoError(format!("RocksDB delete error: {}", e)))?;
        Ok(())
    }

    fn merge_cf(&self, key: &[u8], value: &[u8]) -> Result<(), BackendError> {
        if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.merge_cf(&cf, key, value)
        } else {
            self.db.merge(key, value)
        }
        .map_err(|e| BackendError::IoError(format!("RocksDB merge error: {}", e)))?;
        Ok(())
    }

    fn delete_range_cf(&self, start: &[u8], end: &[u8]) -> Result<(), BackendError> {
        let mut batch = WriteBatch::default();

        if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            batch.delete_range_cf(&cf, start, end);
        } else {
            batch.delete_range(start, end);
        }

        self.db
            .write(batch)
            .map_err(|e| BackendError::IoError(format!("RocksDB delete_range error: {}", e)))?;
        Ok(())
    }
}

/// Merge operator: append operation
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

impl StateStore for RocksDBStateStore {
    fn put_state(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), BackendError> {
        self.put_cf(&key, &value)
    }

    fn get_state(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, BackendError> {
        self.get_cf(&key)
    }

    fn delete_state(&self, key: Vec<u8>) -> Result<(), BackendError> {
        self.delete_cf(&key)
    }

    fn list_states(
        &self,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError> {
        let mut keys = Vec::new();

        let iter = if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.iterator_cf(
                &cf,
                IteratorMode::From(&start_inclusive, rocksdb::Direction::Forward),
            )
        } else {
            self.db.iterator(IteratorMode::From(
                &start_inclusive,
                rocksdb::Direction::Forward,
            ))
        };

        for item in iter {
            let (key, _) =
                item.map_err(|e| BackendError::IoError(format!("RocksDB iterator error: {}", e)))?;
            let key_slice = key.as_ref();

            if key_slice >= start_inclusive.as_slice() && key_slice < end_exclusive.as_slice() {
                keys.push(key.to_vec());
            } else if key_slice >= end_exclusive.as_slice() {
                break;
            }
        }

        Ok(keys)
    }

    fn merge(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), BackendError> {
        let key_bytes = build_key(&key_group, &key, &namespace, &user_key);
        self.merge_cf(&key_bytes, &value)
    }

    fn delete_prefix_bytes(&self, prefix: Vec<u8>) -> Result<usize, BackendError> {
        if prefix.is_empty() {
            return Err(BackendError::Other("Empty prefix not allowed".to_string()));
        }

        if is_all_0xff(&prefix) {
            let iter = if let Some(ref cf_name) = self.column_family_name {
                let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                    BackendError::Other(format!("Column family '{}' not found", cf_name))
                })?;
                self.db.iterator_cf(
                    &cf,
                    IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                )
            } else {
                self.db
                    .iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward))
            };

            let mut batch = WriteBatch::default();
            let mut count = 0;

            for item in iter {
                let (key, _) = item
                    .map_err(|e| BackendError::IoError(format!("RocksDB iterator error: {}", e)))?;
                let key_slice = key.as_ref();

                if key_slice.starts_with(&prefix) {
                    if let Some(ref cf_name) = self.column_family_name {
                        let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                            BackendError::Other(format!("Column family '{}' not found", cf_name))
                        })?;
                        batch.delete_cf(&cf, key_slice);
                    } else {
                        batch.delete(key_slice);
                    }
                    count += 1;
                } else {
                    break;
                }
            }

            if count > 0 {
                self.db.write(batch).map_err(|e| {
                    BackendError::IoError(format!("RocksDB batch write error: {}", e))
                })?;
            }

            return Ok(count);
        }

        let end_key = increment_key(&prefix);
        self.delete_range_cf(&prefix, &end_key)?;

        Ok(0)
    }

    fn scan(&self, prefix: Vec<u8>) -> Result<Box<dyn StateIterator>, BackendError> {
        Ok(Box::new(RocksDBStateIterator {
            db: self.db.clone(),
            column_family_name: self.column_family_name.clone(),
            prefix,
            current: Arc::new(Mutex::new(None)),
            last_key: Arc::new(Mutex::new(None)),
        }))
    }
}

/// RocksDB state iterator
///
/// Uses RocksDB's native iterator directly without loading all data into memory
/// Uses internal state to track current iteration position
struct RocksDBStateIterator {
    /// RocksDB database instance
    db: Arc<DB>,
    /// Column family name (if using column family)
    column_family_name: Option<String>,
    /// Prefix (for filtering)
    prefix: Vec<u8>,
    /// Currently cached key-value pair (for has_next check)
    current: Arc<Mutex<Option<(Vec<u8>, Vec<u8>)>>>,
    /// Last accessed key (for continuing from last position)
    last_key: Arc<Mutex<Option<Vec<u8>>>>,
}

impl RocksDBStateIterator {
    /// Find the next matching item
    fn find_next(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError> {
        let last_key = self
            .last_key
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        let start_key = if let Some(ref last) = *last_key {
            IteratorMode::From(last.as_slice(), rocksdb::Direction::Forward)
        } else {
            IteratorMode::From(&self.prefix, rocksdb::Direction::Forward)
        };

        let iter = if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.iterator_cf(&cf, start_key)
        } else {
            self.db.iterator(start_key)
        };

        for item in iter {
            let (key, value) =
                item.map_err(|e| BackendError::IoError(format!("RocksDB iterator error: {}", e)))?;
            let key_slice = key.as_ref();

            if let Some(ref last) = *last_key
                && key_slice <= last.as_slice()
            {
                continue;
            }

            if key_slice.starts_with(&self.prefix) {
                drop(last_key);
                let mut last_key = self
                    .last_key
                    .lock()
                    .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
                *last_key = Some(key.to_vec());
                return Ok(Some((key.to_vec(), value.to_vec())));
            } else if key_slice > self.prefix.as_slice() {
                break;
            }
        }

        Ok(None)
    }
}

impl StateIterator for RocksDBStateIterator {
    fn has_next(&mut self) -> Result<bool, BackendError> {
        {
            let current = self
                .current
                .lock()
                .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
            if current.is_some() {
                return Ok(true);
            }
        }

        if let Some(pair) = self.find_next()? {
            let mut current = self
                .current
                .lock()
                .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
            *current = Some(pair);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError> {
        {
            let mut current = self
                .current
                .lock()
                .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
            if let Some(pair) = current.take() {
                return Ok(Some(pair));
            }
        }

        self.find_next()
    }
}
