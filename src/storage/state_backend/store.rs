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

// State Store - 状态存储接口
//
// 提供完整的状态存储接口，包括简单 KV、复杂 KV 和迭代器

use crate::storage::state_backend::error::BackendError;

/// 状态存储迭代器
pub trait StateIterator: Send + Sync {
    /// 检查是否还有下一个元素
    ///
    /// # 返回值
    /// - `Ok(true)`: 还有下一个元素
    /// - `Ok(false)`: 没有更多数据
    /// - `Err(BackendError)`: 检查失败
    fn has_next(&mut self) -> Result<bool, BackendError>;

    /// 获取下一个键值对
    ///
    /// # 返回值
    /// - `Ok(Some((key, value)))`: 下一个键值对
    /// - `Ok(None)`: 没有更多数据
    /// - `Err(BackendError)`: 迭代失败
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError>;
}

/// 状态存储接口
///
/// 提供完整的状态存储功能，包括：
/// - 简单 KV 操作（直接使用字节数组作为键）
/// - 复杂 KV 操作（使用复杂键）
/// - 迭代器支持
pub trait StateStore: Send + Sync {
    /// 打开存储（静态方法，由实现类提供）
    ///
    /// # 参数
    /// - `name`: 存储名称或路径
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateStore>)`: 成功打开
    /// - `Err(BackendError)`: 打开失败
    // 注意：Rust 中 trait 不能有静态方法，所以这个方法需要在实现类型上提供

    // --- Simple KV (Bytes) ---

    /// 存储键值对（简单键）
    ///
    /// # 参数
    /// - `key`: 键（字节数组）
    /// - `value`: 值（字节数组）
    ///
    /// # 返回值
    /// - `Ok(())`: 存储成功
    /// - `Err(BackendError)`: 存储失败
    fn put_state(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), BackendError>;

    /// 获取值（简单键）
    ///
    /// # 参数
    /// - `key`: 键（字节数组）
    ///
    /// # 返回值
    /// - `Ok(Some(value))`: 找到值
    /// - `Ok(None)`: 键不存在
    /// - `Err(BackendError)`: 获取失败
    fn get_state(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, BackendError>;

    /// 删除键值对（简单键）
    ///
    /// # 参数
    /// - `key`: 键（字节数组）
    ///
    /// # 返回值
    /// - `Ok(())`: 删除成功
    /// - `Err(BackendError)`: 删除失败
    fn delete_state(&self, key: Vec<u8>) -> Result<(), BackendError>;

    /// 列出指定范围内的所有键
    ///
    /// # 参数
    /// - `start_inclusive`: 起始键（包含）
    /// - `end_exclusive`: 结束键（不包含）
    ///
    /// # 返回值
    /// - `Ok(keys)`: 键列表
    /// - `Err(BackendError)`: 列出失败
    fn list_states(
        &self,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError>;

    // --- Complex KV ---

    /// 存储键值对（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    /// - `user_key`: 用户键（字节数组）
    /// - `value`: 值（字节数组）
    ///
    /// # 返回值
    /// - `Ok(())`: 存储成功
    /// - `Err(BackendError)`: 存储失败
    fn put(
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
        self.put_state(key_bytes, value)
    }

    /// 获取值（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    /// - `user_key`: 用户键（字节数组）
    ///
    /// # 返回值
    /// - `Ok(Some(value))`: 找到值
    /// - `Ok(None)`: 键不存在
    /// - `Err(BackendError)`: 获取失败
    fn get(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );
        self.get_state(key_bytes)
    }

    /// 删除键值对（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    /// - `user_key`: 用户键（字节数组）
    ///
    /// # 返回值
    /// - `Ok(())`: 删除成功
    /// - `Err(BackendError)`: 删除失败
    fn delete(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
    ) -> Result<(), BackendError> {
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );
        self.delete_state(key_bytes)
    }

    /// 合并值（复杂键，使用 merge 操作）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    /// - `user_key`: 用户键（字节数组）
    /// - `value`: 要合并的值（字节数组）
    ///
    /// # 返回值
    /// - `Ok(())`: 合并成功
    /// - `Err(BackendError)`: 合并失败
    fn merge(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), BackendError>;

    /// 删除指定前缀的所有键（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    ///
    /// # 返回值
    /// - `Ok(count)`: 删除的键数量
    /// - `Err(BackendError)`: 删除失败
    fn delete_prefix(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<usize, BackendError> {
        let prefix_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &[],
        );
        self.delete_prefix_bytes(prefix_bytes)
    }

    /// 列出指定范围内的所有键（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    /// - `start_inclusive`: 起始 user_key（包含）
    /// - `end_exclusive`: 结束 user_key（不包含）
    ///
    /// # 返回值
    /// - `Ok(keys)`: 键列表（返回完整的复杂键字节数组）
    /// - `Err(BackendError)`: 列出失败
    fn list_complex(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError> {
        // 构建起始和结束键
        let start_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &start_inclusive,
        );
        let end_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &end_exclusive,
        );
        self.list_states(start_key, end_key)
    }

    /// 删除指定前缀的所有键（字节数组）
    ///
    /// # 参数
    /// - `prefix`: 键前缀（字节数组）
    ///
    /// # 返回值
    /// - `Ok(count)`: 删除的键数量
    /// - `Err(BackendError)`: 删除失败
    fn delete_prefix_bytes(&self, prefix: Vec<u8>) -> Result<usize, BackendError>;

    // --- Iterator ---

    /// 扫描指定前缀的所有键值对（简单键）
    ///
    /// # 参数
    /// - `prefix`: 键前缀（字节数组）
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateIterator>)`: 迭代器
    /// - `Err(BackendError)`: 创建迭代器失败
    fn scan(&self, prefix: Vec<u8>) -> Result<Box<dyn StateIterator>, BackendError>;

    /// 扫描指定前缀的所有键值对（复杂键）
    ///
    /// # 参数
    /// - `key_group`: 键组（字节数组）
    /// - `key`: 键（字节数组）
    /// - `namespace`: 命名空间（字节数组）
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateIterator>)`: 迭代器
    /// - `Err(BackendError)`: 创建迭代器失败
    fn scan_complex(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<Box<dyn StateIterator>, BackendError> {
        let prefix = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &[],
        );
        self.scan(prefix)
    }
}
