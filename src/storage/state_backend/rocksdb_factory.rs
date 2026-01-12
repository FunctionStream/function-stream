// RocksDB State Store Factory - RocksDB 状态存储工厂
//
// 提供创建和配置 RocksDB 状态存储实例的工厂方法

use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::factory::StateStoreFactory;
use crate::storage::state_backend::rocksdb_store::RocksDBStateStore;
use rocksdb::{DB, Options};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// RocksDB 配置选项
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct RocksDBConfig {
    // 注意：不再使用 dir_name，数据库直接存储在任务目录下
    /// 最大打开文件数
    pub max_open_files: Option<i32>,
    /// 写缓冲区大小（字节）
    pub write_buffer_size: Option<usize>,
    /// 最大写缓冲区数量
    pub max_write_buffer_number: Option<i32>,
    /// 目标文件大小基数（字节）
    pub target_file_size_base: Option<u64>,
    /// Level 0 最大字节数（字节）
    pub max_bytes_for_level_base: Option<u64>,
    // 注意：当前不支持压缩配置，使用默认的 none 压缩（不压缩）
}


/// 从配置结构体转换为 RocksDBConfig
impl From<&crate::config::storage::RocksDBStorageConfig> for RocksDBConfig {
    fn from(config: &crate::config::storage::RocksDBStorageConfig) -> Self {
        Self {
            max_open_files: config.max_open_files,
            write_buffer_size: config.write_buffer_size,
            max_write_buffer_number: config.max_write_buffer_number,
            target_file_size_base: config.target_file_size_base,
            max_bytes_for_level_base: config.max_bytes_for_level_base,
        }
    }
}

/// RocksDB 状态存储工厂
pub struct RocksDBStateStoreFactory {
    /// RocksDB 数据库实例
    db: Arc<DB>,
    /// 用于保护列族创建操作的锁
    cf_creation_lock: Mutex<()>,
}

impl StateStoreFactory for RocksDBStateStoreFactory {
    fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn crate::storage::state_backend::store::StateStore>, BackendError> {
        self.new_state_store(column_family)
    }
}

impl RocksDBStateStoreFactory {
    /// 创建新的 RocksDB 状态存储工厂
    ///
    /// # 参数
    /// - `db_path`: 数据库路径
    /// - `config`: RocksDB 配置
    ///
    /// # 返回值
    /// - `Ok(RocksDBStateStoreFactory)`: 成功创建
    /// - `Err(BackendError)`: 创建失败
    pub fn new<P: AsRef<Path>>(db_path: P, config: RocksDBConfig) -> Result<Self, BackendError> {
        // 创建 RocksDB 选项
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // 设置配置选项
        if let Some(max_open_files) = config.max_open_files {
            opts.set_max_open_files(max_open_files);
        }
        if let Some(write_buffer_size) = config.write_buffer_size {
            opts.set_write_buffer_size(write_buffer_size);
        }
        if let Some(max_write_buffer_number) = config.max_write_buffer_number {
            opts.set_max_write_buffer_number(max_write_buffer_number);
        }
        if let Some(target_file_size_base) = config.target_file_size_base {
            opts.set_target_file_size_base(target_file_size_base);
        }
        if let Some(max_bytes_for_level_base) = config.max_bytes_for_level_base {
            opts.set_max_bytes_for_level_base(max_bytes_for_level_base);
        }

        // 注意：当前不支持压缩配置，使用默认的 none 压缩（不压缩）

        // 设置 MergeOperator
        opts.set_merge_operator_associative("appendOp", merge_operator);

        // 确保目录存在
        let db_path = db_path.as_ref();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BackendError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        // 打开数据库
        let db = DB::open(&opts, db_path)
            .map_err(|e| BackendError::IoError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self {
            db: Arc::new(db),
            cf_creation_lock: Mutex::new(()),
        })
    }

    /// 创建新的状态存储实例
    ///
    /// # 参数
    /// - `column_family`: 可选的列族名称，如果为 None 则使用默认列族
    ///
    /// # 返回值
    /// - `Ok(Box<dyn StateStore>)`: 成功创建
    /// - `Err(BackendError)`: 创建失败
    ///
    /// 注意：如果指定了列族名称且不存在，会自动创建该列族
    pub fn new_state_store(
        &self,
        column_family: Option<String>,
    ) -> Result<Box<dyn crate::storage::state_backend::store::StateStore>, BackendError> {
        // 如果指定了列族，确保它存在（不存在则自动创建）
        if let Some(ref cf_name) = column_family
            && cf_name != "default" && self.db.cf_handle(cf_name).is_none() {
                // 获取锁以避免并发创建相同列族
                let _guard = self.cf_creation_lock.lock().map_err(|e| {
                    BackendError::Other(format!("Failed to acquire cf creation lock: {}", e))
                })?;

                // 双重检查：在锁内再次检查列族是否存在
                if self.db.cf_handle(cf_name).is_none() {
                    log::info!("Creating column family '{}' as it does not exist", cf_name);
                    let opts = Options::default();
                    self.db.create_cf(cf_name, &opts).map_err(|e| {
                        BackendError::Other(format!(
                            "Failed to create column family '{}': {}",
                            cf_name, e
                        ))
                    })?;
                }
            }

        RocksDBStateStore::new_with_factory(self.db.clone(), column_family)
    }
}

/// Merge 操作符：用于合并值（追加操作）
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
