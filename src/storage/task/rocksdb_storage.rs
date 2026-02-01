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
//! Uses three column families: task_meta, task_config, task_payload.

use super::storage::{StoredTaskInfo, TaskModuleBytes, TaskStorage};
use crate::config::storage::RocksDBStorageConfig;
use crate::runtime::common::ComponentState;
use anyhow::{anyhow, Context, Result};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, WriteBatch, DB, Options};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

const CF_METADATA: &str = "task_meta";
const CF_CONFIG: &str = "task_config";
const CF_PAYLOAD: &str = "task_payload";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskMetadata {
    task_type: String,
    state: ComponentState,
    created_at: u64,
    checkpoint_id: Option<u64>,
}

pub struct RocksDBTaskStorage {
    db: Arc<DB>,
}

impl RocksDBTaskStorage {
    pub fn new<P: AsRef<Path>>(db_path: P, config: Option<&RocksDBStorageConfig>) -> Result<Self> {
        let path = db_path.as_ref();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        if let Some(cfg) = config {
            Self::apply_tuning_parameters(&mut db_opts, cfg);
        }

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_METADATA, Options::default()),
            ColumnFamilyDescriptor::new(CF_CONFIG, Options::default()),
            ColumnFamilyDescriptor::new(CF_PAYLOAD, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .with_context(|| format!("Failed to open RocksDB at {:?}", path))?;

        Ok(Self { db: Arc::new(db) })
    }

    fn apply_tuning_parameters(opts: &mut Options, cfg: &RocksDBStorageConfig) {
        if let Some(v) = cfg.max_open_files {
            opts.set_max_open_files(v);
        }
        if let Some(v) = cfg.write_buffer_size {
            opts.set_write_buffer_size(v);
        }
        if let Some(v) = cfg.max_write_buffer_number {
            opts.set_max_write_buffer_number(v);
        }
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    }

    fn get_cf(&self, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("Storage integrity error: CF '{}' missing", name))
    }
}

impl TaskStorage for RocksDBTaskStorage {
    fn create_task(&self, task_info: &StoredTaskInfo) -> Result<()> {
        let key = task_info.name.as_bytes();
        let cf_meta = self.get_cf(CF_METADATA)?;
        let cf_conf = self.get_cf(CF_CONFIG)?;
        let cf_payl = self.get_cf(CF_PAYLOAD)?;

        if self.db.get_cf(&cf_meta, key)?.is_some() {
            return Err(anyhow!("Task uniqueness violation: {}", task_info.name));
        }

        let meta = TaskMetadata {
            task_type: task_info.task_type.clone(),
            state: task_info.state.clone(),
            created_at: task_info.created_at,
            checkpoint_id: task_info.checkpoint_id,
        };

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_meta, key, bincode::serialize(&meta)?);
        batch.put_cf(&cf_conf, key, &task_info.config_bytes);

        if let Some(ref module) = task_info.module_bytes {
            batch.put_cf(&cf_payl, key, bincode::serialize(module)?);
        }

        self.db
            .write(batch)
            .context("Atomic transaction failed during task creation")
    }

    fn update_task_state(&self, task_name: &str, new_state: ComponentState) -> Result<()> {
        let cf = self.get_cf(CF_METADATA)?;
        let key = task_name.as_bytes();

        let raw = self
            .db
            .get_cf(&cf, key)?
            .ok_or_else(|| anyhow!("Task {} not found", task_name))?;

        let mut meta: TaskMetadata = bincode::deserialize(&raw)?;
        meta.state = new_state;

        self.db.put_cf(&cf, key, bincode::serialize(&meta)?)?;
        Ok(())
    }

    fn update_task_checkpoint_id(&self, task_name: &str, checkpoint_id: Option<u64>) -> Result<()> {
        let cf = self.get_cf(CF_METADATA)?;
        let key = task_name.as_bytes();

        let raw = self
            .db
            .get_cf(&cf, key)?
            .ok_or_else(|| anyhow!("Task {} not found", task_name))?;

        let mut meta: TaskMetadata = bincode::deserialize(&raw)?;
        meta.checkpoint_id = checkpoint_id;

        self.db.put_cf(&cf, key, bincode::serialize(&meta)?)?;
        Ok(())
    }

    fn delete_task(&self, task_name: &str) -> Result<()> {
        let key = task_name.as_bytes();
        let mut batch = WriteBatch::default();

        batch.delete_cf(&self.get_cf(CF_METADATA)?, key);
        batch.delete_cf(&self.get_cf(CF_CONFIG)?, key);
        batch.delete_cf(&self.get_cf(CF_PAYLOAD)?, key);

        self.db.write(batch).context("Atomic deletion failed")
    }

    fn load_task(&self, task_name: &str) -> Result<StoredTaskInfo> {
        let key = task_name.as_bytes();

        let meta_raw = self
            .db
            .get_cf(&self.get_cf(CF_METADATA)?, key)?
            .ok_or_else(|| anyhow!("Metadata missing: {}", task_name))?;

        let config_bytes = self
            .db
            .get_cf(&self.get_cf(CF_CONFIG)?, key)?
            .ok_or_else(|| anyhow!("Config missing: {}", task_name))?;

        let module_bytes = self
            .db
            .get_cf(&self.get_cf(CF_PAYLOAD)?, key)?
            .and_then(|b| bincode::deserialize::<TaskModuleBytes>(&b).ok());

        let meta: TaskMetadata = bincode::deserialize(&meta_raw)?;

        Ok(StoredTaskInfo {
            name: task_name.to_string(),
            task_type: meta.task_type,
            module_bytes,
            config_bytes: config_bytes.to_vec(),
            state: meta.state,
            created_at: meta.created_at,
            checkpoint_id: meta.checkpoint_id,
        })
    }

    fn list_all_tasks(&self) -> Result<Vec<StoredTaskInfo>> {
        let cf_meta = self.get_cf(CF_METADATA)?;
        let iter = self.db.iterator_cf(&cf_meta, IteratorMode::Start);
        let mut tasks = Vec::new();

        for item in iter {
            let (key, _) = item?;
            let name = std::str::from_utf8(&key)?;
            if let Ok(task) = self.load_task(name) {
                tasks.push(task);
            }
        }
        Ok(tasks)
    }

    fn task_exists(&self, task_name: &str) -> Result<bool> {
        let cf = self.get_cf(CF_METADATA)?;
        Ok(self.db.get_cf(&cf, task_name.as_bytes())?.is_some())
    }
}
