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

// Memory State Store - 内存状态存储实现
//
// 基于 HashMap 实现 StateStore 接口

use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::store::{StateIterator, StateStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 内存状态存储
pub struct MemoryStateStore {
    /// 内部存储
    storage: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStateStore {
    /// 创建新的内存状态存储
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStateStore {
    fn put_state(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), BackendError> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
        storage.insert(key, value);
        Ok(())
    }

    fn get_state(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, BackendError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
        Ok(storage.get(&key).cloned())
    }

    fn delete_state(&self, key: Vec<u8>) -> Result<(), BackendError> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
        storage.remove(&key);
        Ok(())
    }

    fn list_states(
        &self,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        let mut keys: Vec<Vec<u8>> = storage
            .keys()
            .filter(|k| *k >= &start_inclusive && *k < &end_exclusive)
            .cloned()
            .collect();

        keys.sort();
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
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );

        // 获取现有值
        let existing = self.get_state(key_bytes.clone())?;

        // 简单的合并策略：将新值追加到现有值后面（用分隔符）
        let merged = if let Some(existing_value) = existing {
            let mut result = existing_value;
            result.push(b'\0'); // 使用 null 字节作为分隔符
            result.extend_from_slice(&value);
            result
        } else {
            value
        };

        self.put_state(key_bytes, merged)
    }

    fn delete_prefix_bytes(&self, prefix: Vec<u8>) -> Result<usize, BackendError> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        let keys_to_delete: Vec<Vec<u8>> = storage
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        let count = keys_to_delete.len();
        for key in keys_to_delete {
            storage.remove(&key);
        }

        Ok(count)
    }

    fn scan(&self, prefix: Vec<u8>) -> Result<Box<dyn StateIterator>, BackendError> {
        // 获取所有匹配前缀的键值对
        let storage = self
            .storage
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        let mut pairs = Vec::new();
        for (key, value) in storage.iter() {
            if key.starts_with(&prefix) {
                pairs.push((key.clone(), value.clone()));
            }
        }

        Ok(Box::new(MemoryStateIterator {
            pairs: Arc::new(Mutex::new(pairs)),
            index: Arc::new(Mutex::new(0)),
        }))
    }
}

/// 内存状态迭代器
struct MemoryStateIterator {
    pairs: Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>,
    index: Arc<Mutex<usize>>,
}

impl StateIterator for MemoryStateIterator {
    fn has_next(&mut self) -> Result<bool, BackendError> {
        let pairs = self
            .pairs
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
        let index = self
            .index
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        Ok(*index < pairs.len())
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError> {
        let pairs = self
            .pairs
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
        let mut index = self
            .index
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        if *index >= pairs.len() {
            return Ok(None);
        }

        let pair = pairs[*index].clone();
        *index += 1;
        Ok(Some(pair))
    }
}
