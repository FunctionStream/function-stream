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

//! Pluggable metadata KV backend (memory, etcd, Redis, …).

use std::collections::HashMap;

use datafusion::common::Result;
use parking_lot::RwLock;

/// Synchronous metadata store for catalog records.
pub trait MetaStore: Send + Sync {
    fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
}

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
    fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.db.write().insert(key.to_string(), value);
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.read().get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.db.write().remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let db = self.db.read();
        Ok(db
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}
