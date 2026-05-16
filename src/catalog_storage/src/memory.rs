use std::collections::HashMap;

use function_stream_catalog::{CatalogResult, MetaStore};
use parking_lot::RwLock;

/// In-process KV store for single-node deployments and tests.
pub struct InMemoryMetaStore {
    db: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryMetaStore {
    pub fn new() -> Self {
        Self {
            db: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaStore for InMemoryMetaStore {
    fn put(&self, key: &str, value: Vec<u8>) -> CatalogResult<()> {
        self.db.write().insert(key.to_string(), value);
        Ok(())
    }

    fn get(&self, key: &str) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.db.read().get(key).cloned())
    }

    fn delete(&self, key: &str) -> CatalogResult<()> {
        self.db.write().remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> CatalogResult<Vec<(String, Vec<u8>)>> {
        let db = self.db.read();
        Ok(db
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn write_batch(&self, batch: Vec<(String, Option<Vec<u8>>)>) -> CatalogResult<()> {
        let mut db = self.db.write();
        for (key, value) in batch {
            match value {
                Some(value) => {
                    db.insert(key, value);
                }
                None => {
                    db.remove(&key);
                }
            }
        }
        Ok(())
    }
}
