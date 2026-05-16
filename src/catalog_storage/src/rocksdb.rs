use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use function_stream_catalog::{CatalogResult, MetaStore};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

/// RocksDB-backed catalog metadata store.
pub struct RocksDbMetaStore {
    db: Arc<DB>,
}

impl RocksDbMetaStore {
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("stream catalog: create parent directory {parent:?}"))?;
        }
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)
            .with_context(|| format!("stream catalog: open RocksDB at {}", path.display()))?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl MetaStore for RocksDbMetaStore {
    fn put(&self, key: &str, value: Vec<u8>) -> CatalogResult<()> {
        self.db
            .put(key.as_bytes(), value.as_slice())
            .map_err(|e| format!("stream catalog store put: {e}").into())
    }

    fn get(&self, key: &str) -> CatalogResult<Option<Vec<u8>>> {
        self.db
            .get(key.as_bytes())
            .map_err(|e| format!("stream catalog store get: {e}").into())
    }

    fn delete(&self, key: &str) -> CatalogResult<()> {
        self.db
            .delete(key.as_bytes())
            .map_err(|e| format!("stream catalog store delete: {e}").into())
    }

    fn scan_prefix(&self, prefix: &str) -> CatalogResult<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item.map_err(|e| format!("stream catalog store scan: {e}"))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|e| format!("stream catalog store: invalid utf8 key: {e}"))?;
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key, value.to_vec()));
        }
        Ok(out)
    }

    fn write_batch(&self, batch: Vec<(String, Option<Vec<u8>>)>) -> CatalogResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut write_batch = WriteBatch::default();
        for (key, value) in batch {
            match value {
                Some(value) => write_batch.put(key.as_bytes(), value.as_slice()),
                None => write_batch.delete(key.as_bytes()),
            }
        }

        self.db
            .write(write_batch)
            .map_err(|e| format!("stream catalog store write_batch: {e}").into())
    }
}
