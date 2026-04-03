// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RocksDB-backed [`super::MetaStore`] for durable stream catalog rows.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use datafusion::common::Result;
use rocksdb::{DB, Direction, IteratorMode, Options};

use super::MetaStore;

/// Single-node durable KV used by [`crate::storage::stream_catalog::CatalogManager`].
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
    fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.db.put(key.as_bytes(), value.as_slice()).map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!("stream catalog store put: {e}"))
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(key.as_bytes()).map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!("stream catalog store get: {e}"))
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.db.delete(key.as_bytes()).map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!(
                "stream catalog store delete: {e}"
            ))
        })
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward));
        for item in iter {
            let (k, v) = item.map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "stream catalog store scan: {e}"
                ))
            })?;
            let key = String::from_utf8(k.to_vec()).map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "stream catalog store: invalid utf8 key: {e}"
                ))
            })?;
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key, v.to_vec()));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    #[test]
    fn put_get_scan_roundtrip() {
        let dir: PathBuf =
            std::env::temp_dir().join(format!("fs_stream_catalog_test_{}", Uuid::new_v4()));
        let _ = std::fs::remove_dir_all(&dir);

        let store = RocksDbMetaStore::open(&dir).expect("open");
        store.put("catalog:stream_table:a", vec![1, 2, 3]).unwrap();
        store.put("catalog:stream_table:b", vec![4]).unwrap();
        store.put("other:x", vec![9]).unwrap();

        assert_eq!(
            store.get("catalog:stream_table:a").unwrap(),
            Some(vec![1, 2, 3])
        );

        let prefixed = store.scan_prefix("catalog:stream_table:").unwrap();
        assert_eq!(prefixed.len(), 2);
        assert!(prefixed.iter().any(|(k, _)| k.ends_with(":a")));
        assert!(prefixed.iter().any(|(k, _)| k.ends_with(":b")));

        store.delete("catalog:stream_table:a").unwrap();
        assert!(store.get("catalog:stream_table:a").unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
