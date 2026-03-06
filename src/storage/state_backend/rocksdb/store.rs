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
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
};
use std::path::Path;
use std::sync::Arc;

pub struct RocksDBStateStore {
    db: Arc<DB>,
    cf_name: String,
    write_opts: WriteOptions,
}

impl RocksDBStateStore {
    pub fn new_with_factory(
        db: Arc<DB>,
        column_family: Option<String>,
    ) -> Result<Box<dyn StateStore>, BackendError> {
        let cf_name = column_family.unwrap_or_else(|| "default".to_string());
        if db.cf_handle(&cf_name).is_none() {
            return Err(BackendError::Other(format!(
                "Column family '{}' does not exist",
                cf_name
            )));
        }

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);
        write_opts.disable_wal(true);

        Ok(Box::new(Self {
            db,
            cf_name,
            write_opts,
        }))
    }

    pub fn open<P: AsRef<Path>>(path: P, cf_name: Option<String>) -> Result<Self, BackendError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_merge_operator_associative("appendOp", merge_operator);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_enable_pipelined_write(true);
        opts.increase_parallelism(num_cpus::get() as i32);

        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_size(16 * 1024);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_block_cache(&Cache::new_lru_cache(256 * 1024 * 1024));
        opts.set_block_based_table_factory(&block_opts);

        let target_cf = cf_name.unwrap_or_else(|| "default".to_string());

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(&target_cf, opts.clone()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| BackendError::IoError(e.to_string()))?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        Ok(Self {
            db: Arc::new(db),
            cf_name: target_cf,
            write_opts,
        })
    }

    #[inline(always)]
    fn cf_handle(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, BackendError> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| BackendError::Other(format!("Handle for CF '{}' invalid", self.cf_name)))
    }
}

impl StateStore for RocksDBStateStore {
    fn put_state(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), BackendError> {
        let cf = self.cf_handle()?;
        self.db
            .put_cf_opt(&cf, key, value, &self.write_opts)
            .map_err(|e| BackendError::IoError(e.to_string()))
    }

    fn get_state(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, BackendError> {
        let cf = self.cf_handle()?;
        self.db
            .get_cf(&cf, key)
            .map_err(|e| BackendError::IoError(e.to_string()))
    }

    fn delete_state(&self, key: Vec<u8>) -> Result<(), BackendError> {
        let cf = self.cf_handle()?;
        self.db
            .delete_cf_opt(&cf, key, &self.write_opts)
            .map_err(|e| BackendError::IoError(e.to_string()))
    }

    fn list_states(&self, start: Vec<u8>, end: Vec<u8>) -> Result<Vec<Vec<u8>>, BackendError> {
        let cf = self.cf_handle()?;
        let mut ropts = ReadOptions::default();
        ropts.set_iterate_upper_bound(end.clone());
        ropts.set_readahead_size(2 * 1024 * 1024);

        let iter =
            self.db
                .iterator_cf_opt(&cf, ropts, IteratorMode::From(&start, Direction::Forward));
        let mut results = Vec::with_capacity(1024);

        for item in iter {
            let (k, _) = item.map_err(|e| BackendError::IoError(e.to_string()))?;
            if k.as_ref() >= end.as_slice() {
                break;
            }
            results.push(k.to_vec());
        }
        Ok(results)
    }

    fn merge(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), BackendError> {
        let cf = self.cf_handle()?;
        let full_key = build_key(&key_group, &key, &namespace, &user_key);
        self.db
            .merge_cf_opt(&cf, full_key, value, &self.write_opts)
            .map_err(|e| BackendError::IoError(e.to_string()))
    }

    fn delete_prefix_bytes(&self, prefix: Vec<u8>) -> Result<usize, BackendError> {
        if prefix.is_empty() {
            return Err(BackendError::Other("Empty prefix".into()));
        }
        let cf = self.cf_handle()?;

        if !is_all_0xff(&prefix) {
            let end_key = increment_key(&prefix);
            self.db
                .delete_range_cf_opt(&cf, &prefix, &end_key, &self.write_opts)
                .map_err(|e| BackendError::IoError(e.to_string()))?;
            return Ok(0);
        }

        let mut batch = WriteBatch::default();
        let mut count = 0;
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);

        for item in iter {
            let (k, _) = item.map_err(|e| BackendError::IoError(e.to_string()))?;
            if !k.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(&cf, k);
            count += 1;
            if count % 1000 == 0 {
                self.db
                    .write_opt(batch, &self.write_opts)
                    .map_err(|e| BackendError::IoError(e.to_string()))?;
                batch = WriteBatch::default();
            }
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| BackendError::IoError(e.to_string()))?;
        Ok(count)
    }

    fn scan(&self, prefix: Vec<u8>) -> Result<Box<dyn StateIterator>, BackendError> {
        Ok(Box::new(RocksDBStateIterator::new(
            self.db.clone(),
            self.cf_name.clone(),
            prefix,
        )?))
    }
}

pub struct RocksDBStateIterator {
    _db: Arc<DB>,
    buffer: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl RocksDBStateIterator {
    fn new(db: Arc<DB>, cf_name: String, prefix: Vec<u8>) -> Result<Self, BackendError> {
        let mut collected = Vec::with_capacity(512);
        {
            let cf = db
                .cf_handle(&cf_name)
                .ok_or_else(|| BackendError::Other("CF missing".into()))?;
            let mut ropts = ReadOptions::default();
            ropts.set_prefix_same_as_start(true);
            ropts.set_readahead_size(1024 * 1024);

            let iter =
                db.iterator_cf_opt(&cf, ropts, IteratorMode::From(&prefix, Direction::Forward));

            for item in iter {
                let (k, v) = item.map_err(|e| BackendError::IoError(e.to_string()))?;
                if !k.starts_with(&prefix) {
                    break;
                }
                collected.push((k.to_vec(), v.to_vec()));
            }
        }

        Ok(Self {
            _db: db,
            buffer: collected.into_iter(),
        })
    }
}

impl StateIterator for RocksDBStateIterator {
    fn has_next(&mut self) -> Result<bool, BackendError> {
        Ok(!self.buffer.as_slice().is_empty())
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError> {
        Ok(self.buffer.next())
    }
}

fn merge_operator(
    _: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let size = existing.map_or(0, |v| v.len()) + operands.iter().map(|o| o.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(size);
    if let Some(v) = existing {
        buf.extend_from_slice(v);
    }
    for op in operands {
        buf.extend_from_slice(op);
    }
    Some(buf)
}
