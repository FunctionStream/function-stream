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

use super::error::{Result, StateEngineError};
use super::io_manager::{CompactJob, IoManager, SpillJob};
use super::metrics::StateMetricsCollector;
use crate::runtime::memory::{MemoryBlock, MemoryTicket};
use arrow_array::builder::{BinaryBuilder, BooleanBuilder, UInt64Builder};
use arrow_array::{Array, BinaryArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use crossbeam_channel::TrySendError;
use parking_lot::{Mutex, RwLock};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use uuid::Uuid;

pub(crate) const PARTITION_KEY_COL: &str = "__fs_partition_key";

pub type PartitionKey = Vec<u8>;
pub type MemTable = HashMap<PartitionKey, Vec<RecordBatch>>;
pub type TombstoneMap = HashMap<PartitionKey, u64>;

const TOMBSTONE_ENTRY_OVERHEAD: usize = 8 + 16;

pub struct OperatorStateStore {
    pub operator_id: u32,
    current_epoch: AtomicU64,

    active_table: RwLock<MemTable>,
    immutable_tables: Mutex<VecDeque<(u64, MemTable)>>,

    data_files: RwLock<Vec<PathBuf>>,
    tombstone_files: RwLock<Vec<PathBuf>>,
    tombstones: RwLock<TombstoneMap>,

    state_ticket: Arc<MemoryTicket>,
    state_used: AtomicU64,
    state_quota: u64,
    soft_limit: u64,
    io_manager: IoManager,

    data_dir: PathBuf,
    tombstone_dir: PathBuf,

    spill_notify: Arc<Notify>,
    is_spilling: AtomicBool,
    is_compacting: AtomicBool,
}

const DEFAULT_SOFT_LIMIT_RATIO: f64 = 0.7;

impl OperatorStateStore {
    /// `pipeline_state_memory_block` is the pipeline-wide slab reserved at job spawn; this store
    /// takes one ticket of `operator_state_memory_bytes` from it.
    pub fn new(
        operator_id: u32,
        base_dir: impl AsRef<Path>,
        io_manager: IoManager,
        pipeline_state_memory_block: Arc<MemoryBlock>,
        operator_state_memory_bytes: u64,
    ) -> Result<Arc<Self>> {
        let ticket = pipeline_state_memory_block
            .try_allocate(operator_state_memory_bytes)
            .ok_or_else(|| {
                StateEngineError::Corruption(
                    "pipeline state memory block exhausted (operator state ticket)".into(),
                )
            })?;
        let state_ticket = Arc::new(ticket);
        let soft_limit = (operator_state_memory_bytes as f64 * DEFAULT_SOFT_LIMIT_RATIO) as u64;

        let op_dir = base_dir.as_ref().join(format!("op_{operator_id}"));
        let data_dir = op_dir.join("data");
        let tombstone_dir = op_dir.join("tombstones");

        fs::create_dir_all(&data_dir).map_err(StateEngineError::IoError)?;
        fs::create_dir_all(&tombstone_dir).map_err(StateEngineError::IoError)?;

        Ok(Arc::new(Self {
            operator_id,
            current_epoch: AtomicU64::new(1),
            active_table: RwLock::new(HashMap::new()),
            immutable_tables: Mutex::new(VecDeque::new()),
            data_files: RwLock::new(Vec::new()),
            tombstone_files: RwLock::new(Vec::new()),
            tombstones: RwLock::new(HashMap::new()),
            state_ticket,
            state_used: AtomicU64::new(0),
            state_quota: operator_state_memory_bytes,
            soft_limit,
            io_manager,
            data_dir,
            tombstone_dir,
            spill_notify: Arc::new(Notify::new()),
            is_spilling: AtomicBool::new(false),
            is_compacting: AtomicBool::new(false),
        }))
    }

    fn state_bytes_used(&self) -> u64 {
        self.state_used.load(Ordering::Relaxed)
    }

    fn state_should_spill(&self) -> bool {
        self.state_bytes_used() > self.soft_limit
    }

    fn rebuild_state_used_from_tables(&self) {
        let mut n = 0u64;
        for rows in self.active_table.read().values() {
            for b in rows {
                n += b.get_array_memory_size() as u64;
            }
        }
        for (_, table) in self.immutable_tables.lock().iter() {
            for rows in table.values() {
                for b in rows {
                    n += b.get_array_memory_size() as u64;
                }
            }
        }
        self.state_used.store(n, Ordering::Release);
    }

    async fn wait_until_memory_available_async(self: Arc<Self>, need: u64) {
        while self.state_used.load(Ordering::Relaxed).saturating_add(need) > self.state_quota {
            self.trigger_spill();
            self.spill_notify.notified().await;
        }
    }

    fn wait_until_memory_available_blocking(self: &Arc<Self>, need: u64) -> Result<()> {
        loop {
            if self.state_used.load(Ordering::Relaxed).saturating_add(need) <= self.state_quota {
                return Ok(());
            }
            self.trigger_spill();
            let start = Instant::now();
            while self.is_spilling.load(Ordering::SeqCst) {
                if start.elapsed() > Duration::from_secs(120) {
                    return Err(StateEngineError::Corruption(
                        "state memory wait for spill timed out".into(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    pub async fn put(self: &Arc<Self>, key: PartitionKey, batch: RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size() as u64;
        self.clone().wait_until_memory_available_async(size).await;
        self.state_used.fetch_add(size, Ordering::Relaxed);
        self.active_table
            .write()
            .entry(key)
            .or_default()
            .push(batch);

        if self.state_should_spill() {
            self.downgrade_active_table(self.current_epoch.load(Ordering::Acquire));
            self.trigger_spill();
        }
        Ok(())
    }

    pub fn remove_batches(self: &Arc<Self>, key: PartitionKey) -> Result<()> {
        let current_ep = self.current_epoch.load(Ordering::Acquire);
        let tombstone_mem_size = (key.len() + TOMBSTONE_ENTRY_OVERHEAD) as u64;

        {
            let mut tb_guard = self.tombstones.write();
            if !tb_guard.contains_key(&key) {
                self.wait_until_memory_available_blocking(tombstone_mem_size)?;
                self.state_used
                    .fetch_add(tombstone_mem_size, Ordering::Relaxed);
                tb_guard.insert(key.clone(), current_ep);
            }
        }

        let released_active: u64 = self
            .active_table
            .write()
            .remove(&key)
            .map(|rows| rows.iter().map(|b| b.get_array_memory_size() as u64).sum())
            .unwrap_or(0);

        let mut released_imm = 0u64;
        for (_, table) in self.immutable_tables.lock().iter_mut() {
            if let Some(rows) = table.remove(&key) {
                released_imm += rows
                    .iter()
                    .map(|b| b.get_array_memory_size() as u64)
                    .sum::<u64>();
            }
        }

        let released = released_active.saturating_add(released_imm);
        if released > 0 {
            self.state_used.fetch_sub(released, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Checkpoint phase 1: flush mutable in-memory state into an epoch-tagged immutable table and
    /// trigger spill. This does NOT advance `current_epoch`.
    pub fn prepare_checkpoint_epoch(self: &Arc<Self>, epoch: u64) -> Result<()> {
        self.downgrade_active_table(epoch);
        self.trigger_spill();
        Ok(())
    }

    /// Checkpoint phase 2: once global metadata commit succeeds, advance the durable safe epoch.
    pub fn commit_checkpoint_epoch(self: &Arc<Self>, epoch: u64) -> Result<()> {
        self.current_epoch
            .store(epoch.saturating_add(1), Ordering::Release);
        Ok(())
    }

    /// Checkpoint rollback: do not advance `current_epoch`. Any already-spilled files are kept and
    /// filtered by safe epoch during restore.
    pub fn abort_checkpoint_epoch(self: &Arc<Self>, _epoch: u64) -> Result<()> {
        Ok(())
    }

    /// Backward-compatible helper (phase1 + phase2 in one call).
    pub fn snapshot_epoch(self: &Arc<Self>, epoch: u64) -> Result<()> {
        self.prepare_checkpoint_epoch(epoch)?;
        self.commit_checkpoint_epoch(epoch)?;
        Ok(())
    }

    pub async fn await_spill_complete(&self) {
        while self.is_spilling.load(Ordering::SeqCst) {
            self.spill_notify.notified().await;
        }
    }

    fn downgrade_active_table(&self, epoch: u64) {
        let mut active_guard = self.active_table.write();
        if active_guard.is_empty() {
            return;
        }
        let old_active = std::mem::take(&mut *active_guard);
        self.immutable_tables.lock().push_back((epoch, old_active));
    }

    pub async fn get_batches(&self, key: &[u8]) -> Result<Vec<RecordBatch>> {
        let deleted_epoch = self.tombstones.read().get(key).copied();
        let mut out = Vec::new();

        if let Some(batches) = self.active_table.read().get(key) {
            out.extend(batches.clone());
        }

        for (table_epoch, table) in self.immutable_tables.lock().iter().rev() {
            if let Some(del_ep) = deleted_epoch
                && *table_epoch <= del_ep
            {
                continue;
            }
            if let Some(batches) = table.get(key) {
                out.extend(batches.clone());
            }
        }

        let paths: Vec<PathBuf> = self.data_files.read().clone();
        if paths.is_empty() {
            return Ok(out);
        }

        let pk = key.to_vec();
        let merged = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
            let mut acc = Vec::new();
            for path in paths {
                let file_epoch = extract_epoch(&path);
                if let Some(del_ep) = deleted_epoch
                    && file_epoch <= del_ep
                {
                    continue;
                }

                // Native Bloom Filter intercepts empty reads here
                let file = File::open(&path).map_err(StateEngineError::IoError)?;
                let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
                for maybe in reader.by_ref() {
                    if let Some(filtered) = filter_and_strip_partition_key(&maybe?, &pk)? {
                        acc.push(filtered);
                    }
                }
            }
            Ok(acc)
        })
        .await
        .map_err(|_| StateEngineError::Corruption("Tokio task panicked".into()))??;

        out.extend(merged);
        Ok(out)
    }

    fn trigger_spill(self: &Arc<Self>) {
        if !self.is_spilling.swap(true, Ordering::SeqCst) {
            let target = self.immutable_tables.lock().pop_front();
            let Some((epoch, data)) = target else {
                self.is_spilling.store(false, Ordering::SeqCst);
                self.spill_notify.notify_waiters();
                return;
            };

            let tombstone_snapshot = self.tombstones.read().clone();
            let job = SpillJob {
                store: self.clone(),
                epoch,
                data,
                tombstone_snapshot,
            };

            match self.io_manager.try_send_spill(job) {
                Ok(()) => {}
                Err(TrySendError::Full(j)) | Err(TrySendError::Disconnected(j)) => {
                    self.immutable_tables.lock().push_front((j.epoch, j.data));
                    self.is_spilling.store(false, Ordering::SeqCst);
                    self.spill_notify.notify_waiters();
                }
            }
        }
    }

    pub fn trigger_minor_compaction(self: &Arc<Self>) {
        if !self.is_compacting.swap(true, Ordering::SeqCst) {
            let _ = self.io_manager.try_send_compact(CompactJob::Minor {
                store: self.clone(),
            });
        }
    }

    pub fn trigger_major_compaction(self: &Arc<Self>) {
        if !self.is_compacting.swap(true, Ordering::SeqCst) {
            let _ = self.io_manager.try_send_compact(CompactJob::Major {
                store: self.clone(),
            });
        }
    }

    pub(crate) fn execute_spill_sync(
        self: &Arc<Self>,
        epoch: u64,
        data: MemTable,
        tombstones: TombstoneMap,
        metrics: &Arc<dyn StateMetricsCollector>,
    ) -> Result<()> {
        let mut batches_to_write = Vec::new();
        let mut spilled_bytes: u64 = 0;
        let distinct_keys_count = data.len() as u64;

        for (key, batches) in data {
            for batch in batches {
                spilled_bytes += batch.get_array_memory_size() as u64;
                batches_to_write.push(inject_partition_key(&batch, &key)?);
            }
        }

        if !batches_to_write.is_empty() {
            let path = self.data_dir.join(Self::generate_data_file_name(epoch));
            if let Err(e) =
                write_parquet_with_bloom_atomic(&path, &batches_to_write, distinct_keys_count)
            {
                metrics.inc_io_errors(self.operator_id);
                let restored = restore_memtable_from_injected_batches(batches_to_write)?;
                self.immutable_tables.lock().push_front((epoch, restored));
                self.rebuild_state_used_from_tables();
                self.is_spilling.store(false, Ordering::SeqCst);
                self.spill_notify.notify_waiters();
                return Err(e);
            }
            self.data_files.write().push(path);
        }

        if !tombstones.is_empty() {
            let mut key_builder = BinaryBuilder::new();
            let mut epoch_builder = UInt64Builder::new();
            let tomb_ndv = tombstones.len() as u64;

            for (key, del_epoch) in tombstones.iter() {
                key_builder.append_value(key);
                epoch_builder.append_value(*del_epoch);
            }

            let schema = Arc::new(Schema::new(vec![
                Field::new("deleted_key", DataType::Binary, false),
                Field::new("deleted_epoch", DataType::UInt64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(key_builder.finish()),
                    Arc::new(epoch_builder.finish()),
                ],
            )?;

            let path = self
                .tombstone_dir
                .join(Self::generate_tombstone_file_name(epoch));
            if let Err(e) = write_parquet_with_bloom_atomic(&path, &[batch], tomb_ndv) {
                metrics.inc_io_errors(self.operator_id);
                return Err(e);
            }
            self.tombstone_files.write().push(path);
        }

        if spilled_bytes > 0 {
            self.state_used.fetch_sub(spilled_bytes, Ordering::Relaxed);
        }

        metrics.record_memory_usage(self.operator_id, self.state_bytes_used());

        self.is_spilling.store(false, Ordering::SeqCst);
        self.spill_notify.notify_waiters();

        if !self.immutable_tables.lock().is_empty() {
            self.trigger_spill();
        }
        Ok(())
    }

    pub(crate) fn execute_compact_sync(
        self: &Arc<Self>,
        is_major: bool,
        metrics: &Arc<dyn StateMetricsCollector>,
    ) -> Result<()> {
        let result = (|| -> Result<()> {
            let files_to_merge = {
                let df = self.data_files.read();
                if df.len() < 2 {
                    return Ok(());
                }
                if is_major {
                    df.clone()
                } else {
                    df.iter().take(2).cloned().collect()
                }
            };

            let tombstone_snapshot = self.tombstones.read().clone();
            let compacted_watermark_epoch = files_to_merge
                .iter()
                .map(|p| extract_epoch(p))
                .max()
                .unwrap_or(0);
            let new_path = self
                .data_dir
                .join(Self::generate_data_file_name(compacted_watermark_epoch));

            let mut all_batches = Vec::new();
            let mut estimated_rows = 0;

            for path in &files_to_merge {
                let file_epoch = extract_epoch(path);
                let file = File::open(path).map_err(StateEngineError::IoError)?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
                for batch in reader {
                    let b = batch?;
                    if let Some(filtered) =
                        filter_tombstones_from_batch(&b, &tombstone_snapshot, file_epoch)?
                    {
                        estimated_rows += filtered.num_rows() as u64;
                        all_batches.push(filtered);
                    }
                }
            }

            if !all_batches.is_empty() {
                if let Err(e) = write_parquet_with_bloom_atomic(
                    &new_path,
                    &all_batches,
                    estimated_rows.max(100),
                ) {
                    metrics.inc_io_errors(self.operator_id);
                    return Err(e);
                }
                let mut df = self.data_files.write();
                df.retain(|p| !files_to_merge.contains(p));
                df.push(new_path);
            } else {
                let mut df = self.data_files.write();
                df.retain(|p| !files_to_merge.contains(p));
            }

            for path in &files_to_merge {
                let _ = fs::remove_file(path);
            }

            {
                let mut tg = self.tombstones.write();
                let keys_before: Vec<PartitionKey> = tg.keys().cloned().collect();
                tg.retain(|_key, deleted_epoch| *deleted_epoch > compacted_watermark_epoch);
                let mut tomb_freed = 0u64;
                for k in keys_before {
                    if !tg.contains_key(&k) {
                        tomb_freed += (k.len() + TOMBSTONE_ENTRY_OVERHEAD) as u64;
                    }
                }
                if tomb_freed > 0 {
                    self.state_used.fetch_sub(tomb_freed, Ordering::Relaxed);
                }
                metrics.record_memory_usage(self.operator_id, self.state_bytes_used());
            }

            {
                let mut tf_guard = self.tombstone_files.write();
                tf_guard.retain(|p| {
                    if extract_epoch(p) <= compacted_watermark_epoch {
                        let _ = fs::remove_file(p);
                        return false;
                    }
                    true
                });
            }

            Ok(())
        })();

        self.is_compacting.store(false, Ordering::SeqCst);
        result
    }

    pub async fn restore_metadata(
        self: &Arc<Self>,
        safe_epoch: u64,
    ) -> Result<HashSet<PartitionKey>> {
        self.state_used.store(0, Ordering::Release);
        self.active_table.write().clear();
        self.immutable_tables
            .lock()
            .retain(|(e, _)| *e <= safe_epoch);

        let cleanup_future = |files: &mut Vec<PathBuf>| {
            files.retain(|path| {
                if extract_epoch(path) > safe_epoch {
                    let _ = fs::remove_file(path);
                    false
                } else {
                    true
                }
            });
        };
        cleanup_future(&mut self.data_files.write());
        cleanup_future(&mut self.tombstone_files.write());

        let tomb_paths = self.tombstone_files.read().clone();
        type RawTombstones = HashMap<PartitionKey, u64>;
        let raw_tombstones = tokio::task::spawn_blocking(move || -> Result<RawTombstones> {
            let mut map = RawTombstones::new();
            for path in tomb_paths {
                let file = File::open(&path).map_err(StateEngineError::IoError)?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
                for batch in reader {
                    let batch = batch?;
                    let key_col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    let ep_col = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    for i in 0..key_col.len() {
                        let k = key_col.value(i).to_vec();
                        let e = ep_col.value(i);
                        let current_max = map.get(&k).copied().unwrap_or(0);
                        if e > current_max {
                            map.insert(k, e);
                        }
                    }
                }
            }
            Ok(map)
        })
        .await
        .map_err(|_| StateEngineError::Corruption("Task Panicked".into()))??;

        let tomb_epoch_map = raw_tombstones.clone();

        *self.tombstones.write() = raw_tombstones;
        self.rebuild_state_used_from_tables();
        let tomb_overhead: u64 = self
            .tombstones
            .read()
            .keys()
            .map(|k| (k.len() + TOMBSTONE_ENTRY_OVERHEAD) as u64)
            .sum();
        if tomb_overhead > 0 {
            self.state_used.fetch_add(tomb_overhead, Ordering::Relaxed);
        }

        let data_paths = self.data_files.read().clone();
        let active_keys = tokio::task::spawn_blocking(move || -> Result<HashSet<PartitionKey>> {
            let mut keys = HashSet::new();
            for path in data_paths {
                let file_epoch = extract_epoch(&path);
                let file = File::open(&path).map_err(StateEngineError::IoError)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let schema = builder.parquet_schema();
                let mask = ProjectionMask::leaves(schema, vec![schema.columns().len() - 1]);
                let reader = builder.with_projection(mask).build()?;

                for batch in reader {
                    let batch = batch?;
                    let key_col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    for i in 0..key_col.len() {
                        let k = key_col.value(i).to_vec();
                        let is_active = match tomb_epoch_map.get(&k) {
                            Some(del_ep) => *del_ep < file_epoch,
                            None => true,
                        };
                        if is_active {
                            keys.insert(k);
                        }
                    }
                }
            }
            Ok(keys)
        })
        .await
        .map_err(|_| StateEngineError::Corruption("Task Panicked".into()))??;

        self.current_epoch.store(safe_epoch + 1, Ordering::Release);
        Ok(active_keys)
    }

    // ========================================================================
    // UUID-based file name generators
    // ========================================================================

    fn generate_data_file_name(epoch: u64) -> String {
        format!("data-epoch-{}_uuid-{}.parquet", epoch, Uuid::now_v7())
    }

    fn generate_tombstone_file_name(epoch: u64) -> String {
        format!("tombstone-epoch-{}_uuid-{}.parquet", epoch, Uuid::now_v7())
    }
}

// ============================================================================
// Internal helper functions
// ============================================================================

fn write_parquet_with_bloom_atomic(path: &Path, batches: &[RecordBatch], ndv: u64) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let tmp = path.with_extension("tmp");
    {
        let file = File::create(&tmp).map_err(StateEngineError::IoError)?;
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_ndv(ndv)
            .build();

        let mut writer = ArrowWriter::try_new(&file, batches[0].schema(), Some(props))?;
        for b in batches {
            writer.write(b)?;
        }
        writer.close()?;
        file.sync_all().map_err(StateEngineError::IoError)?;
    }
    fs::rename(&tmp, path).map_err(StateEngineError::IoError)?;
    Ok(())
}

fn extract_epoch(path: &Path) -> u64 {
    let name = path
        .file_name()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    if let Some(start) = name.find("-epoch-") {
        let after = &name[start + 7..];
        if let Some(end) = after.find("_uuid-") {
            return after[..end].parse().unwrap_or(0);
        }
    }
    0
}

fn inject_partition_key(batch: &RecordBatch, key: &[u8]) -> Result<RecordBatch> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        PARTITION_KEY_COL,
        DataType::Binary,
        false,
    )));
    let schema = Arc::new(Schema::new(fields));
    let key_array = Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
        key,
        batch.num_rows(),
    )));
    let mut cols = batch.columns().to_vec();
    cols.push(key_array as Arc<dyn Array>);
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn filter_tombstones_from_batch(
    batch: &RecordBatch,
    tombstones: &TombstoneMap,
    file_epoch: u64,
) -> Result<Option<RecordBatch>> {
    if tombstones.is_empty() {
        return Ok(Some(batch.clone()));
    }
    let Ok(idx) = batch.schema().index_of(PARTITION_KEY_COL) else {
        return Ok(Some(batch.clone()));
    };

    let key_col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
    let mut has_valid = false;

    for i in 0..batch.num_rows() {
        let key = key_col.value(i).to_vec();
        let keep = match tombstones.get(&key) {
            Some(deleted_epoch) => *deleted_epoch < file_epoch,
            None => true,
        };
        mask_builder.append_value(keep);
        if keep {
            has_valid = true;
        }
    }

    if !has_valid {
        return Ok(None);
    }
    let mask = mask_builder.finish();
    Ok(Some(arrow::compute::filter_record_batch(batch, &mask)?))
}

fn filter_and_strip_partition_key(
    batch: &RecordBatch,
    target_key: &[u8],
) -> Result<Option<RecordBatch>> {
    let Ok(idx) = batch.schema().index_of(PARTITION_KEY_COL) else {
        return Ok(Some(batch.clone()));
    };
    let key_col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        mask_builder.append_value(key_col.value(i) == target_key);
    }
    let mask = mask_builder.finish();
    let filtered = arrow::compute::filter_record_batch(batch, &mask)?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }
    let mut proj: Vec<usize> = (0..filtered.num_columns()).collect();
    proj.retain(|&i| i != idx);
    Ok(Some(filtered.project(&proj)?))
}

fn restore_memtable_from_injected_batches(batches: Vec<RecordBatch>) -> Result<MemTable> {
    let mut m = MemTable::new();
    for batch in batches {
        let idx = batch.schema().index_of(PARTITION_KEY_COL).unwrap();
        let key_col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let pk = key_col.value(0).to_vec();
        let mut proj: Vec<usize> = (0..batch.num_columns()).collect();
        proj.retain(|&i| i != idx);
        let projected = batch.project(&proj)?;
        m.entry(pk).or_default().push(projected);
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::super::io_manager::IoPool;
    use super::super::metrics::NoopMetricsCollector;
    use super::*;
    use crate::runtime::memory::{MemoryBlock, MemoryPool, global_memory_pool};
    use arrow_array::Int64Array;
    use tempfile::TempDir;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn make_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    const TEST_OPERATOR_MEMORY: u64 = 2 * 1024 * 1024;

    fn ensure_global_memory_pool() {
        use crate::runtime::memory::{init_global_memory_pool, try_global_memory_pool};
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            if try_global_memory_pool().is_err() {
                init_global_memory_pool(TEST_OPERATOR_MEMORY.saturating_mul(64))
                    .expect("global memory pool init");
            }
        });
    }

    fn state_block(bytes: u64) -> Arc<MemoryBlock> {
        ensure_global_memory_pool();
        global_memory_pool()
            .try_request_block(bytes)
            .expect("test pipeline state memory block")
    }

    fn setup() -> (TempDir, IoManager, IoPool) {
        ensure_global_memory_pool();
        let tmp = TempDir::new().unwrap();
        let metrics: Arc<dyn StateMetricsCollector> = Arc::new(NoopMetricsCollector);
        let (pool, mgr) = IoPool::try_new(1, 1, metrics).unwrap();
        (tmp, mgr, pool)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let key = b"key-a".to_vec();
        let batch = make_batch(&[10, 20, 30]);
        store.put(key.clone(), batch).await.unwrap();

        let result = store.get_batches(&key).await.unwrap();
        assert_eq!(result.len(), 1);
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[10, 20, 30]);
    }

    #[tokio::test]
    async fn test_multiple_puts_same_key() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let key = b"key-x".to_vec();
        store.put(key.clone(), make_batch(&[1])).await.unwrap();
        store.put(key.clone(), make_batch(&[2])).await.unwrap();

        let result = store.get_batches(&key).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let result = store.get_batches(b"no-such-key").await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_remove_batches() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let key = b"key-del".to_vec();
        store.put(key.clone(), make_batch(&[42])).await.unwrap();

        store.remove_batches(key.clone()).unwrap();

        let result = store.get_batches(&key).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_remove_does_not_affect_other_keys() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let k1 = b"key-1".to_vec();
        let k2 = b"key-2".to_vec();
        store.put(k1.clone(), make_batch(&[1])).await.unwrap();
        store.put(k2.clone(), make_batch(&[2])).await.unwrap();

        store.remove_batches(k1.clone()).unwrap();

        assert!(store.get_batches(&k1).await.unwrap().is_empty());
        assert_eq!(store.get_batches(&k2).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_epoch_advances() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        store.put(b"k".to_vec(), make_batch(&[1])).await.unwrap();
        store.snapshot_epoch(5).unwrap();

        assert_eq!(store.current_epoch.load(Ordering::Acquire), 6);
    }

    #[tokio::test]
    async fn test_data_survives_snapshot_via_spill() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let key = b"persist".to_vec();
        store.put(key.clone(), make_batch(&[99])).await.unwrap();
        store.snapshot_epoch(1).unwrap();
        store.await_spill_complete().await;

        let result = store.get_batches(&key).await.unwrap();
        assert!(!result.is_empty());
        let col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[99]);
    }

    #[tokio::test]
    async fn test_tombstone_hides_immutable_data() {
        let (tmp, mgr, _pool) = setup();
        let store = OperatorStateStore::new(
            1,
            tmp.path(),
            mgr,
            state_block(TEST_OPERATOR_MEMORY),
            TEST_OPERATOR_MEMORY,
        )
        .unwrap();

        let key = b"will-die".to_vec();
        store.put(key.clone(), make_batch(&[7])).await.unwrap();

        // Move to immutable via snapshot
        store.snapshot_epoch(1).unwrap();

        // Tombstone at epoch 2 (> immutable epoch 1)
        store.current_epoch.store(2, Ordering::Release);
        store.remove_batches(key.clone()).unwrap();

        let result = store.get_batches(&key).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_state_block_tracking() {
        let mem = MemoryPool::new(2048);
        assert_eq!(mem.usage_metrics().0, 0);

        mem.force_reserve(100);
        assert_eq!(mem.usage_metrics().0, 100);

        mem.force_release(40);
        assert_eq!(mem.usage_metrics().0, 60);

        let soft_limit = 1000u64;
        assert!(mem.usage_metrics().0 <= soft_limit);
        mem.force_reserve(1000);
        assert!(mem.usage_metrics().0 > soft_limit);
    }

    #[tokio::test]
    async fn test_state_block_hard_limit() {
        let mem = MemoryPool::new(1024);
        assert!(mem.usage_metrics().0 + 500 <= mem.usage_metrics().1);
        assert!(mem.usage_metrics().0 + 1025 > mem.usage_metrics().1);

        mem.force_reserve(800);
        assert!(mem.usage_metrics().0 + 300 > mem.usage_metrics().1);
        assert!(mem.usage_metrics().0 + 200 <= mem.usage_metrics().1);
    }

    #[test]
    fn test_extract_epoch() {
        let path = PathBuf::from("/tmp/data-epoch-42_uuid-abc123.parquet");
        assert_eq!(extract_epoch(&path), 42);

        let path2 = PathBuf::from("/tmp/tombstone-epoch-100_uuid-def456.parquet");
        assert_eq!(extract_epoch(&path2), 100);

        let path3 = PathBuf::from("/tmp/random-file.parquet");
        assert_eq!(extract_epoch(&path3), 0);
    }

    #[test]
    fn test_inject_and_strip_partition_key() {
        let batch = make_batch(&[1, 2, 3]);
        let key = b"pk-test";

        let injected = inject_partition_key(&batch, key).unwrap();
        assert_eq!(injected.num_columns(), 2);
        assert!(injected.schema().index_of(PARTITION_KEY_COL).is_ok());

        let stripped = filter_and_strip_partition_key(&injected, key)
            .unwrap()
            .unwrap();
        assert_eq!(stripped.num_columns(), 1);
        let col = stripped
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_filter_partition_key_mismatch() {
        let batch = make_batch(&[1, 2]);
        let injected = inject_partition_key(&batch, b"pk-a").unwrap();

        let result = filter_and_strip_partition_key(&injected, b"pk-b").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_restore_memtable_roundtrip() {
        let batch1 = inject_partition_key(&make_batch(&[10]), b"k1").unwrap();
        let batch2 = inject_partition_key(&make_batch(&[20]), b"k2").unwrap();
        let batch3 = inject_partition_key(&make_batch(&[30]), b"k1").unwrap();

        let restored =
            restore_memtable_from_injected_batches(vec![batch1, batch2, batch3]).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[b"k1".as_ref()].len(), 2);
        assert_eq!(restored[b"k2".as_ref()].len(), 1);
    }

    #[test]
    fn test_write_and_read_parquet() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.parquet");

        let batch = make_batch(&[100, 200, 300]);
        write_parquet_with_bloom_atomic(&path, std::slice::from_ref(&batch), 1).unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let read_batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(read_batches.len(), 1);
        let col = read_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[100, 200, 300]);
    }

    #[test]
    fn test_filter_tombstones_from_batch() {
        let batch = make_batch(&[1, 2, 3]);
        let key = b"victim";
        let injected = inject_partition_key(&batch, key).unwrap();

        let mut tombstones: TombstoneMap = HashMap::new();
        tombstones.insert(key.to_vec(), 10);

        // file_epoch <= tombstone epoch => fully filtered
        let result = filter_tombstones_from_batch(&injected, &tombstones, 5).unwrap();
        assert!(result.is_none());

        // file_epoch > tombstone epoch => data survives
        let result = filter_tombstones_from_batch(&injected, &tombstones, 15).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_write_empty_batches_is_noop() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("empty.parquet");

        write_parquet_with_bloom_atomic(&path, &[], 0).unwrap();
        assert!(!path.exists());
    }
}
