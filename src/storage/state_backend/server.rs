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

// State Storage Server - 状态存储服务器
//
// 提供统一的状态存储管理服务

use crate::config::storage::StateStorageConfig;
use crate::storage::state_backend::STATE_DIR_NAME;
use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::factory::{
    FactoryType, StateStoreFactory, get_factory_for_task,
};
use std::sync::Arc;

/// 状态存储服务器
///
/// 根据配置创建状态存储工厂
pub struct StateStorageServer {
    /// 状态存储配置
    config: StateStorageConfig,
}

impl StateStorageServer {
    /// 创建新的状态存储服务器
    ///
    /// # 参数
    /// - `config`: 状态存储配置
    ///
    /// # 返回值
    /// - `Ok(StateStorageServer)`: 成功创建
    /// - `Err(BackendError)`: 创建失败
    pub fn new(config: StateStorageConfig) -> Result<Self, BackendError> {
        // 验证 RocksDB 需要 base_dir
        let factory_type = match config.storage_type {
            crate::config::storage::StateStorageType::Memory => FactoryType::Memory,
            crate::config::storage::StateStorageType::RocksDB => FactoryType::RocksDB,
        };

        if factory_type == FactoryType::RocksDB && config.base_dir.is_none() {
            return Err(BackendError::Other(
                "base_dir is required for RocksDB factory type".to_string(),
            ));
        }

        // 如果是 RocksDB，创建 base_dir/state/ 目录
        if factory_type == FactoryType::RocksDB
            && let Some(ref base_dir) = config.base_dir {
                let state_dir = std::path::Path::new(base_dir).join(STATE_DIR_NAME);
                std::fs::create_dir_all(&state_dir).map_err(|e| {
                    BackendError::IoError(format!("Failed to create state directory: {}", e))
                })?;
            }

        Ok(Self { config })
    }

    /// 创建状态存储工厂
    ///
    /// # 参数
    /// - `task_name`: 任务名称
    /// - `created_at`: 创建时间（Unix 时间戳，秒）
    ///
    /// # 返回值
    /// - `Ok(Arc<dyn StateStoreFactory>)`: 成功创建工厂
    /// - `Err(BackendError)`: 创建失败
    pub fn create_factory(
        &self,
        task_name: String,
        created_at: u64,
    ) -> Result<Arc<dyn StateStoreFactory>, BackendError> {
        let factory_type = match self.config.storage_type {
            crate::config::storage::StateStorageType::Memory => FactoryType::Memory,
            crate::config::storage::StateStorageType::RocksDB => FactoryType::RocksDB,
        };

        // 转换 RocksDB 配置
        let rocksdb_config = if factory_type == FactoryType::RocksDB {
            Some(
                crate::storage::state_backend::rocksdb_factory::RocksDBConfig::from(
                    &self.config.rocksdb,
                ),
            )
        } else {
            None
        };

        // 构建 base_dir/state/ 路径
        let state_dir = if factory_type == FactoryType::RocksDB {
            self.config
                .base_dir
                .as_ref()
                .map(|base_dir| std::path::Path::new(base_dir).join(STATE_DIR_NAME))
        } else {
            None
        };

        get_factory_for_task(
            factory_type,
            task_name,
            created_at,
            state_dir.as_deref(),
            rocksdb_config,
        )
    }
}
