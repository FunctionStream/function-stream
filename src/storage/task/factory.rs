// Task Storage Factory - 任务存储工厂
//
// 提供创建任务存储实例的工厂方法，根据配置创建相应的存储实现

use super::rocksdb_storage::RocksDBTaskStorage;
use super::storage::TaskStorage;
use crate::config::find_or_create_data_dir;
use crate::config::storage::{TaskStorageConfig, TaskStorageType};
use anyhow::{Context, Result};
use std::path::Path;

/// 任务存储工厂
pub struct TaskStorageFactory;

impl TaskStorageFactory {
    /// 根据配置创建任务存储实例
    ///
    /// # 参数
    /// - `config`: 任务存储配置
    /// - `task_name`: 任务名称（用于构建默认路径）
    ///
    /// # 返回值
    /// - `Ok(Box<dyn TaskStorage>)`: 成功创建存储实例
    /// - `Err(...)`: 创建失败
    pub fn create_storage(
        config: &TaskStorageConfig,
        task_name: &str,
    ) -> Result<Box<dyn TaskStorage>> {
        match config.storage_type {
            TaskStorageType::RocksDB => {
                // 确定数据库路径
                let db_path = if let Some(ref path) = config.db_path {
                    // 使用配置中指定的路径
                    Path::new(path).to_path_buf()
                } else {
                    // 使用默认路径：data/task/{task_name}
                    let data_dir = find_or_create_data_dir()
                        .context("Failed to find or create data directory")?;
                    data_dir.join("task").join(task_name)
                };

                // 确保目录存在
                if let Some(parent) = db_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context(format!("Failed to create directory: {:?}", parent))?;
                }

                // 创建 RocksDB 存储实例
                let storage = RocksDBTaskStorage::new(db_path, Some(&config.rocksdb))?;
                Ok(Box::new(storage))
            }
        }
    }
}
