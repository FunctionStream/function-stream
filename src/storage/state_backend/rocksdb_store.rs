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

// RocksDB State Store - RocksDB 状态存储实现
//
// 基于 RocksDB 实现 StateStore 接口，支持列族和优化操作

use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::key_builder::{build_key, increment_key, is_all_0xff};
use crate::storage::state_backend::store::{StateIterator, StateStore};
use rocksdb::{DB, IteratorMode, WriteBatch};
use std::sync::{Arc, Mutex};

/// RocksDB 状态存储
pub struct RocksDBStateStore {
    /// RocksDB 数据库实例
    db: Arc<DB>,
    /// 列族名称（如果使用列族）
    ///
    /// 使用 MultiThreaded 模式时，每次操作时通过名称获取句柄
    /// 这样避免了生命周期和指针的问题
    column_family_name: Option<String>,
}

impl RocksDBStateStore {
    /// 从工厂创建新的状态存储实例
    ///
    /// # 参数
    /// - `db`: 数据库实例
    /// - `column_family`: 可选的列族名称
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateStore>)`: 成功创建
    /// - `Err(BackendError)`: 创建失败
    ///
    /// 注意：调用方应确保列族已经存在（工厂会自动创建）
    pub fn new_with_factory(
        db: Arc<DB>,
        column_family: Option<String>,
    ) -> Result<Box<dyn StateStore>, BackendError> {
        // 验证列族存在（如果指定了非默认列族）
        let cf_name = if let Some(ref cf_name) = column_family {
            if cf_name != "default" {
                // 验证列族存在
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

    /// 打开 RocksDB 存储（静态方法，简化版本）
    ///
    /// # 参数
    /// - `name`: 数据库路径
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateStore>)`: 成功打开
    /// - `Err(BackendError)`: 打开失败
    pub fn open<P: AsRef<std::path::Path>>(name: P) -> Result<Box<dyn StateStore>, BackendError> {
        let path = name.as_ref();

        // 创建 RocksDB 选项
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("appendOp", merge_operator);

        // 打开数据库
        let db = DB::open(&opts, path)
            .map_err(|e| BackendError::IoError(format!("Failed to open RocksDB: {}", e)))?;

        Self::new_with_factory(Arc::new(db), None)
    }

    /// 使用列族进行操作的辅助方法
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

/// Merge 操作符：追加操作
fn merge_operator(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    use std::io::Write;

    let mut buf = Vec::new();

    // 先写入所有操作数
    for operand in operands {
        buf.write_all(operand).ok()?;
    }

    // 然后追加现有值（如果存在）
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

        // 创建迭代器
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

        // 检查是否是全 0xFF 前缀（特殊处理）
        if is_all_0xff(&prefix) {
            // 使用迭代器删除
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

        // 使用范围删除（更高效）
        let end_key = increment_key(&prefix);
        self.delete_range_cf(&prefix, &end_key)?;

        // 计算删除的数量（需要迭代统计，或者返回估计值）
        // 为了简化，这里返回 0，实际实现可能需要统计
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

/// RocksDB 状态迭代器
///
/// 直接使用 RocksDB 的原生迭代器，不将所有数据加载到内存
/// 使用内部状态跟踪当前迭代位置
struct RocksDBStateIterator {
    /// RocksDB 数据库实例
    db: Arc<DB>,
    /// 列族名称（如果使用列族）
    column_family_name: Option<String>,
    /// 前缀（用于过滤）
    prefix: Vec<u8>,
    /// 当前缓存的键值对（用于 has_next 检查）
    current: Arc<Mutex<Option<(Vec<u8>, Vec<u8>)>>>,
    /// 上次访问的最后一个键（用于从上次位置继续）
    last_key: Arc<Mutex<Option<Vec<u8>>>>,
}

impl RocksDBStateIterator {
    /// 查找下一个匹配的项
    fn find_next(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError> {
        let last_key = self
            .last_key
            .lock()
            .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;

        // 确定迭代的起始位置
        let start_key = if let Some(ref last) = *last_key {
            // 从上次的位置之后开始（排除上次的键）
            IteratorMode::From(last.as_slice(), rocksdb::Direction::Forward)
        } else {
            // 从前缀开始
            IteratorMode::From(&self.prefix, rocksdb::Direction::Forward)
        };

        // 创建迭代器
        let iter = if let Some(ref cf_name) = self.column_family_name {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                BackendError::Other(format!("Column family '{}' not found", cf_name))
            })?;
            self.db.iterator_cf(&cf, start_key)
        } else {
            self.db.iterator(start_key)
        };

        // 查找下一个匹配前缀的项
        for item in iter {
            let (key, value) =
                item.map_err(|e| BackendError::IoError(format!("RocksDB iterator error: {}", e)))?;
            let key_slice = key.as_ref();

            // 跳过上次的键（如果存在）
            if let Some(ref last) = *last_key
                && key_slice <= last.as_slice() {
                    continue;
                }

            if key_slice.starts_with(&self.prefix) {
                // 更新 last_key
                drop(last_key);
                let mut last_key = self
                    .last_key
                    .lock()
                    .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
                *last_key = Some(key.to_vec());
                return Ok(Some((key.to_vec(), value.to_vec())));
            } else if key_slice > self.prefix.as_slice() {
                // 键已经超过前缀范围，没有更多匹配的项
                break;
            }
        }

        Ok(None)
    }
}

impl StateIterator for RocksDBStateIterator {
    fn has_next(&mut self) -> Result<bool, BackendError> {
        // 如果已经有缓存的项，直接返回 true
        {
            let current = self
                .current
                .lock()
                .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
            if current.is_some() {
                return Ok(true);
            }
        }

        // 尝试查找下一个项
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
        // 先检查是否有缓存的项
        {
            let mut current = self
                .current
                .lock()
                .map_err(|e| BackendError::Other(format!("Lock error: {}", e)))?;
            if let Some(pair) = current.take() {
                return Ok(Some(pair));
            }
        }

        // 从迭代器获取下一个项
        self.find_next()
    }
}
