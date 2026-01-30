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

//! RocksDB Task Storage - RocksDB-based task storage implementation
//!
//! Uses RocksDB column family to store task information, with task name as key.

use super::storage::{StoredTaskInfo, TaskStorage};
use crate::config::storage::RocksDBStorageConfig;
use crate::runtime::common::ComponentState;
use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, DB, MergeOperands, Options};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// Column family name
const CF_TASK: &str = "task";

/// Task data (for serialization)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskData {
    #[serde(skip_serializing_if = "Option::is_none")]
    wasm_bytes: Option<Vec<u8>>,
    config_bytes: Vec<u8>,
    state: String, // String representation of ComponentState
    created_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_id: Option<u64>,
}

/// State update operation (for merge)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateUpdate {
    state: String,
}

/// Checkpoint ID update operation (for merge)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointIdUpdate {
    checkpoint_id: Option<u64>,
}

/// RocksDB task storage implementation
pub struct RocksDBTaskStorage {
    /// RocksDB database instance
    db: Arc<DB>,
}

impl RocksDBTaskStorage {
    /// Create a new RocksDB task storage instance
    ///
    /// # Arguments
    /// - `db_path`: RocksDB database path
    /// - `config`: RocksDB configuration options (optional)
    ///
    /// # Returns
    /// - `Ok(RocksDBTaskStorage)`: Successfully created
    /// - `Err(...)`: Creation failed
    pub fn new<P: AsRef<Path>>(db_path: P, config: Option<&RocksDBStorageConfig>) -> Result<Self> {
        let path = db_path.as_ref();

        // Create column family descriptor
        let mut cf_opts = Options::default();
        cf_opts.set_merge_operator_associative("state_merge", state_merge_operator);

        // Apply config to column family options (if needed)
        // Note: column family options typically don't need these configs, main config is on database options

        let cf_descriptor = ColumnFamilyDescriptor::new(CF_TASK, cf_opts);

        // Create database options
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Apply configuration options
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

        // Open database
        let cfs = vec![cf_descriptor];
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .context(format!("Failed to open RocksDB at path: {:?}", path))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get task column family
    fn get_cf(&self) -> std::sync::Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db
            .cf_handle(CF_TASK)
            .expect("Task column family should exist")
    }
}

/// Merge operator: for updating state and checkpoint ID
fn state_merge_operator(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // If existing_val doesn't exist, return None (shouldn't happen, task must be created first)
    let mut task_data: TaskData = if let Some(existing) = existing_val {
        serde_json::from_slice(existing).ok()?
    } else {
        return None;
    };

    // Apply all merge operations
    for operand in operands {
        // Try to parse as state update
        if let Ok(state_update) = serde_json::from_slice::<StateUpdate>(operand) {
            task_data.state = state_update.state;
            continue;
        }

        // Try to parse as checkpoint ID update
        if let Ok(checkpoint_update) = serde_json::from_slice::<CheckpointIdUpdate>(operand) {
            task_data.checkpoint_id = checkpoint_update.checkpoint_id;
            continue;
        }
    }

    // Serialize and return
    serde_json::to_vec(&task_data).ok()
}

impl TaskStorage for RocksDBTaskStorage {
    fn create_task(&self, task_info: &StoredTaskInfo) -> Result<()> {
        let cf = self.get_cf();
        let key = task_info.name.as_bytes();

        // Check if task already exists
        if self.db.get_cf(&cf, key)?.is_some() {
            return Err(anyhow::anyhow!("Task '{}' already exists", task_info.name));
        }

        // Build task data
        let task_data = TaskData {
            wasm_bytes: task_info.wasm_bytes.clone(),
            config_bytes: task_info.config_bytes.clone(),
            state: format!("{:?}", task_info.state),
            created_at: task_info.created_at,
            checkpoint_id: task_info.checkpoint_id,
        };

        // Serialize to JSON
        let value = serde_json::to_vec(&task_data).context("Failed to serialize task data")?;

        // Write to RocksDB
        self.db
            .put_cf(&cf, key, value)
            .context("Failed to write task to RocksDB")?;

        Ok(())
    }

    fn update_task_state(&self, task_name: &str, new_state: ComponentState) -> Result<()> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        // Check if task exists
        if self.db.get_cf(&cf, key)?.is_none() {
            return Err(anyhow::anyhow!("Task '{}' not found", task_name));
        }

        // Build state update
        let state_update = StateUpdate {
            state: format!("{:?}", new_state),
        };

        // Serialize state update
        let merge_value =
            serde_json::to_vec(&state_update).context("Failed to serialize state update")?;

        // Use merge operation to update state
        self.db
            .merge_cf(&cf, key, merge_value)
            .context("Failed to merge state update")?;

        Ok(())
    }

    fn update_task_checkpoint_id(&self, task_name: &str, checkpoint_id: Option<u64>) -> Result<()> {
        let cf = self.get_cf();
        let key = task_name.as_bytes();

        // Check if task exists
        if self.db.get_cf(&cf, key)?.is_none() {
            return Err(anyhow::anyhow!("Task '{}' not found", task_name));
        }

        // Build checkpoint ID update
        let checkpoint_update = CheckpointIdUpdate { checkpoint_id };

        // Serialize checkpoint ID update
        let merge_value = serde_json::to_vec(&checkpoint_update)
            .context("Failed to serialize checkpoint ID update")?;

        // Use merge operation to update checkpoint ID
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
                // Deserialize task data
                let task_data: TaskData =
                    serde_json::from_slice(&value).context("Failed to deserialize task data")?;

                // Parse state string to ComponentState
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

    fn list_tasks(&self) -> Result<Vec<String>> {
        let cf = self.get_cf();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        let mut names = Vec::new();
        for item in iter {
            let (key, _) = item.context("RocksDB iterator error")?;
            if let Ok(name) = std::str::from_utf8(&key) {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }
}

/// Parse ComponentState string
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
            // Handle Error state, may need to parse error message
            Some(ComponentState::Error {
                error: "Unknown error".to_string(),
            })
        }
        _ => None,
    }
}
