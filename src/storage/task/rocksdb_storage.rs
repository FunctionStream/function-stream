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

// RocksDB Task Storage - 基于 RocksDB 的任务存储实现
//
// 使用 RocksDB 列族存储任务信息，Key 为任务名称

use super::storage::{StoredTaskInfo, TaskStorage};
use crate::config::storage::RocksDBStorageConfig;
use crate::runtime::common::ComponentState;
use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, DB, MergeOperands, Options};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// 列族名称
const CF_TASK: &str = "task";

/// 任务数据（用于序列化）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskData {
    #[serde(skip_serializing_if = "Option::is_none")]
    wasm_bytes: Option<Vec<u8>>,
    config_bytes: Vec<u8>,
    state: String, // ComponentState 的字符串表示
    created_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_id: Option<u64>,
}

/// 状态更新操作（用于 merge）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateUpdate {
    state: String,
}

/// 检查点 ID 更新操作（用于 merge）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointIdUpdate {
    checkpoint_id: Option<u64>,
}

/// RocksDB 任务存储实现
pub struct RocksDBTaskStorage {
    /// RocksDB 数据库实例
    db: Arc<DB>,
}

impl RocksDBTaskStorage {
    /// 创建新的 RocksDB 任务存储实例
    ///
    /// # 参数
    /// - `db_path`: RocksDB 数据库路径
    /// - `config`: RocksDB 配置选项（可选）
    ///
    /// # 返回值
    /// - `Ok(RocksDBTaskStorage)`: 成功创建
    /// - `Err(...)`: 创建失败
    pub fn new<P: AsRef<Path>>(db_path: P, config: Option<&RocksDBStorageConfig>) -> Result<Self> {
        let path = db_path.as_ref();

        // 创建列族描述符
        let mut cf_opts = Options::default();
        cf_opts.set_merge_operator_associative("state_merge", state_merge_operator);

        // 应用配置到列族选项（如果需要）
        // 注意：列族选项通常不需要这些配置，主要配置在数据库选项中

        let cf_descriptor = ColumnFamilyDescriptor::new(CF_TASK, cf_opts);

        // 创建数据库选项
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // 应用配置选项
        if let Some(config) = config {
            if let Some(max_open_files) = config.max_open_files {
                db_opts.set_max_open_files(max_open_files);
            }
            if let Some(write_buffer_size) = config.write_buffer_size {
                db_opts.set_write_buffer_size(write_buffer_size);
            }
            if let Some(max_write_buffer_number) = config.max_write_buffer_number {
                db_opts.set_max_write_buffer_number(max_write_buffer_number);
            }
            if let Some(target_file_size_base) = config.target_file_size_base {
                db_opts.set_target_file_size_base(target_file_size_base);
            }
            if let Some(max_bytes_for_level_base) = config.max_bytes_for_level_base {
                db_opts.set_max_bytes_for_level_base(max_bytes_for_level_base);
            }
        }

        // 打开数据库
        let cfs = vec![cf_descriptor];
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .context(format!("Failed to open RocksDB at path: {:?}", path))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// 获取任务列族
    fn get_cf(&self) -> std::sync::Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db
            .cf_handle(CF_TASK)
            .expect("Task column family should exist")
    }
}

/// Merge 操作符：用于更新状态和检查点 ID
fn state_merge_operator(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // 如果 existing_val 不存在，返回 None（不应该发生，因为需要先创建任务）
    let mut task_data: TaskData = if let Some(existing) = existing_val {
        serde_json::from_slice(existing).ok()?
    } else {
        return None;
    };

    // 应用所有 merge 操作
    for operand in operands {
        // 尝试解析为状态更新
        if let Ok(state_update) = serde_json::from_slice::<StateUpdate>(operand) {
            task_data.state = state_update.state;
            continue;
        }

        // 尝试解析为检查点 ID 更新
        if let Ok(checkpoint_update) = serde_json::from_slice::<CheckpointIdUpdate>(operand) {
            task_data.checkpoint_id = checkpoint_update.checkpoint_id;
            continue;
        }
    }

    // 序列化并返回
    serde_json::to_vec(&task_data).ok()
}

impl TaskStorage for RocksDBTaskStorage {
    fn create_task(&self, task_info: &StoredTaskInfo) -> Result<()> {
        let cf = self.get_cf();
        let key = task_info.name.as_bytes();

        // 检查任务是否已存在
        if self.db.get_cf(&cf, key)?.is_some() {
            return Err(anyhow::anyhow!("Task '{}' already exists", task_info.name));
        }

        // 构建任务数据
        let task_data = TaskData {
            wasm_bytes: task_info.wasm_bytes.clone(),
            config_bytes: task_info.config_bytes.clone(),
            state: format!("{:?}", task_info.state),
            created_at: task_info.created_at,
            checkpoint_id: task_info.checkpoint_id,
        };

        // 序列化为 JSON
        let value = serde_json::to_vec(&task_data).context("Failed to serialize task data")?;

        // 写入 RocksDB
        self.db
            .put_cf(&cf, key, value)
            .context("Failed to write task to RocksDB")?;

        Ok(())
    }

    fn update_task_state(&self, task_name: &str, new_state: ComponentState) -> Result<()> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        // 检查任务是否存在
        if self.db.get_cf(&cf, key)?.is_none() {
            return Err(anyhow::anyhow!("Task '{}' not found", task_name));
        }

        // 构建状态更新
        let state_update = StateUpdate {
            state: format!("{:?}", new_state),
        };

        // 序列化状态更新
        let merge_value =
            serde_json::to_vec(&state_update).context("Failed to serialize state update")?;

        // 使用 merge 操作更新状态
        self.db
            .merge_cf(&cf, key, merge_value)
            .context("Failed to merge state update")?;

        Ok(())
    }

    fn update_task_checkpoint_id(&self, task_name: &str, checkpoint_id: Option<u64>) -> Result<()> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        // 检查任务是否存在
        if self.db.get_cf(&cf, key)?.is_none() {
            return Err(anyhow::anyhow!("Task '{}' not found", task_name));
        }

        // 构建检查点 ID 更新
        let checkpoint_update = CheckpointIdUpdate { checkpoint_id };

        // 序列化检查点 ID 更新
        let merge_value = serde_json::to_vec(&checkpoint_update)
            .context("Failed to serialize checkpoint ID update")?;

        // 使用 merge 操作更新检查点 ID
        self.db
            .merge_cf(&cf, key, merge_value)
            .context("Failed to merge checkpoint ID update")?;

        Ok(())
    }

    fn delete_task(&self, task_name: &str) -> Result<()> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        self.db
            .delete_cf(&cf, key)
            .context("Failed to delete task from RocksDB")?;

        Ok(())
    }

    fn load_task(&self, task_name: &str) -> Result<StoredTaskInfo> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        match self.db.get_cf(&cf, key) {
            Ok(Some(value)) => {
                // 反序列化任务数据
                let task_data: TaskData =
                    serde_json::from_slice(&value).context("Failed to deserialize task data")?;

                // 解析状态字符串为 ComponentState
                let state = parse_component_state(&task_data.state)
                    .ok_or_else(|| anyhow::anyhow!("Invalid state: {}", task_data.state))?;

                Ok(StoredTaskInfo {
                    name: task_name.to_string(),
                    wasm_bytes: task_data.wasm_bytes,
                    config_bytes: task_data.config_bytes,
                    state,
                    created_at: task_data.created_at,
                    checkpoint_id: task_data.checkpoint_id,
                })
            }
            Ok(None) => Err(anyhow::anyhow!("Task '{}' not found", task_name)),
            Err(e) => Err(anyhow::anyhow!("Failed to read from RocksDB: {}", e)),
        }
    }

    fn task_exists(&self, task_name: &str) -> Result<bool> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        match self.db.get_cf(&cf, key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(anyhow::anyhow!("Failed to check task existence: {}", e)),
        }
    }
}

/// 解析 ComponentState 字符串
fn parse_component_state(s: &str) -> Option<ComponentState> {
    match s {
        "Uninitialized" => Some(ComponentState::Uninitialized),
        "Initialized" => Some(ComponentState::Initialized),
        "Starting" => Some(ComponentState::Starting),
        "Running" => Some(ComponentState::Running),
        "Checkpointing" => Some(ComponentState::Checkpointing),
        "Stopping" => Some(ComponentState::Stopping),
        "Stopped" => Some(ComponentState::Stopped),
        "Closing" => Some(ComponentState::Closing),
        "Closed" => Some(ComponentState::Closed),
        s if s.starts_with("Error") => {
            // 处理 Error 状态，可能需要解析错误信息
            Some(ComponentState::Error {
                error: "Unknown error".to_string(),
            })
        }
        _ => None,
    }
}
