// State Store Factory - 状态存储工厂接口
//
// 定义统一的工厂接口，用于创建状态存储实例

use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::store::StateStore;
use std::path::Path;
use std::sync::Arc;

/// 状态存储工厂接口
///
/// 所有状态存储工厂都应该实现这个接口
pub trait StateStoreFactory: Send + Sync {
    /// 创建新的状态存储实例
    ///
    /// # 参数
    /// - `column_family`: 可选的列族名称（某些实现可能不支持）
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateStore>)`: 成功创建
    /// - `Err(BackendError)`: 创建失败
    fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn StateStore>, BackendError>;
}

/// 工厂类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryType {
    /// 内存工厂
    Memory,
    /// RocksDB 工厂
    RocksDB,
}

pub fn get_factory_for_task<P: AsRef<Path>>(
    factory_type: FactoryType,
    task_name: String,
    created_at: u64,
    base_dir: Option<P>,
    rocksdb_config: Option<crate::storage::state_backend::rocksdb_factory::RocksDBConfig>,
) -> Result<Arc<dyn StateStoreFactory>, BackendError> {
    match factory_type {
        FactoryType::Memory => {
            // 内存工厂不需要 task_name 和 created_at，返回默认工厂
            Ok(crate::storage::state_backend::memory_factory::MemoryStateStoreFactory::default_factory())
        }
        FactoryType::RocksDB => {
            let base_dir = base_dir.ok_or_else(|| {
                BackendError::Other("base_dir is required for RocksDB factory".to_string())
            })?;

            // 使用 task_name 和 created_at 构建数据库路径
            // base_dir 已经是 base_dir/state/ 目录
            // 路径格式：{base_dir}/{task_name}-{created_at}
            // 例如：data/state/my_task-1234567890
            let db_path = base_dir
                .as_ref()
                .join(format!("{}-{}", task_name, created_at));

            // 使用提供的配置，如果没有则使用默认配置
            let config = rocksdb_config.unwrap_or_default();
            let factory =
                crate::storage::state_backend::rocksdb_factory::RocksDBStateStoreFactory::new(
                    db_path, config,
                )?;

            Ok(Arc::new(factory))
        }
    }
}
