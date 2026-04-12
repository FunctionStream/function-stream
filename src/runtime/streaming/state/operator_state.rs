// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::error::{Result, StateEngineError};
use super::io_manager::{CompactJob, IoManager, SpillJob};
use super::metrics::StateMetricsCollector;
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;
use uuid::Uuid;

pub(crate) const PARTITION_KEY_COL: &str = "__fs_partition_key";

pub type PartitionKey = Vec<u8>;
pub type MemTable = HashMap<PartitionKey, Vec<RecordBatch>>;
pub type TombstoneMap = HashMap<PartitionKey, u64>;

const TOMBSTONE_ENTRY_OVERHEAD: usize = 8 + 16;

#[derive(Debug)]
pub struct MemoryController {
    current_usage: AtomicUsize,
    hard_limit: usize,
    soft_limit: usize,
}

impl MemoryController {
    pub fn new(soft_limit: usize, hard_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            current_usage: AtomicUsize::new(0),
            hard_limit,
            soft_limit,
        })
    }
    pub fn exceeds_hard_limit(&self, incoming: usize) -> bool {
        self.current_usage.load(Ordering::Relaxed) + incoming > self.hard_limit
    }
    pub fn should_spill(&self) -> bool {
        self.current_usage.load(Ordering::Relaxed) > self.soft_limit
    }
    pub fn record_inc(&self, bytes: usize) {
        self.current_usage.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_dec(&self, bytes: usize) {
        self.current_usage.fetch_sub(bytes, Ordering::Relaxed);
    }
    pub fn usage_bytes(&self) -> usize {
        self.current_usage.load(Ordering::Relaxed)
    }
}

pub struct OperatorStateStore {
    pub operator_id: u32,
    current_epoch: AtomicU64,

    active_table: RwLock<MemTable>,
    immutable_tables: Mutex<VecDeque<(u64, MemTable)>>,

    data_files: RwLock<Vec<PathBuf>>,
    tombstone_files: RwLock<Vec<PathBuf>>,
    tombstones: RwLock<TombstoneMap>,

    mem_ctrl: Arc<MemoryController>,
    io_manager: IoManager,

    data_dir: PathBuf,
    tombstone_dir: PathBuf,

    spill_notify: Arc<Notify>,
    is_spilling: AtomicBool,
    is_compacting: AtomicBool,
}

impl OperatorStateStore {
    pub fn new(
        operator_id: u32,
        base_dir: impl AsRef<Path>,
        mem_ctrl: Arc<MemoryController>,
        io_manager: IoManager,
    ) -> Result<Arc<Self>> {
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
            mem_ctrl,
            io_manager,
            data_dir,
            tombstone_dir,
            spill_notify: Arc::new(Notify::new()),
            is_spilling: AtomicBool::new(false),
            is_compacting: AtomicBool::new(false),
        }))
    }

    pub async fn put(self: &Arc<Self>, key: PartitionKey, batch: RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        while self.mem_ctrl.exceeds_hard_limit(size) {
            self.trigger_spill();
            self.spill_notify.notified().await;
        }

        self.mem_ctrl.record_inc(size);
        self.active_table
            .write()
            .entry(key)
            .or_default()
            .push(batch);

        if self.mem_ctrl.should_spill() {
            self.downgrade_active_table(self.current_epoch.load(Ordering::Acquire));
            self.trigger_spill();
        }
        Ok(())
    }

    pub fn remove_batches(&self, key: PartitionKey) -> Result<()> {
        let current_ep = self.current_epoch.load(Ordering::Acquire);
        let tombstone_mem_size = key.len() + TOMBSTONE_ENTRY_OVERHEAD;

        {
            let mut tb_guard = self.tombstones.write();
            if tb_guard.insert(key.clone(), current_ep).is_none() {
                self.mem_ctrl.record_inc(tombstone_mem_size);
            }
        }

        if let Some(batches) = self.active_table.write().remove(&key) {
            let released: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            self.mem_ctrl.record_dec(released);
        }

        let mut imm = self.immutable_tables.lock();
        for (_, table) in imm.iter_mut() {
            if let Some(batches) = table.remove(&key) {
                let released: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
                self.mem_ctrl.record_dec(released);
            }
        }

        Ok(())
    }

    pub fn snapshot_epoch(self: &Arc<Self>, epoch: u64) -> Result<()> {
        self.downgrade_active_table(epoch);
        self.trigger_spill();
        self.current_epoch
            .store(epoch.saturating_add(1), Ordering::Release);
        Ok(())
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
        let mut size_to_release: usize = 0;
        let distinct_keys_count = data.len() as u64;

        for (key, batches) in data {
            for batch in batches {
                size_to_release += batch.get_array_memory_size();
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

        self.mem_ctrl.record_dec(size_to_release);
        metrics.record_memory_usage(self.operator_id, self.mem_ctrl.usage_bytes());

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

            // Watermark GC
            {
                let mut tg = self.tombstones.write();
                let mut memory_freed = 0;

                tg.retain(|key, deleted_epoch| {
                    if *deleted_epoch <= compacted_watermark_epoch {
                        memory_freed += key.len() + TOMBSTONE_ENTRY_OVERHEAD;
                        false
                    } else {
                        true
                    }
                });

                if memory_freed > 0 {
                    self.mem_ctrl.record_dec(memory_freed);
                    metrics.record_memory_usage(self.operator_id, self.mem_ctrl.usage_bytes());
                }
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

    pub async fn restore_metadata(&self, safe_epoch: u64) -> Result<HashSet<PartitionKey>> {
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
        let loaded_tombstones = tokio::task::spawn_blocking(move || -> Result<TombstoneMap> {
            let mut map = HashMap::new();
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

        let mut total_tombstone_mem = 0;
        for key in loaded_tombstones.keys() {
            total_tombstone_mem += key.len() + TOMBSTONE_ENTRY_OVERHEAD;
        }
        self.mem_ctrl.record_inc(total_tombstone_mem);
        *self.tombstones.write() = loaded_tombstones.clone();

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
                        let is_active = match loaded_tombstones.get(&k) {
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
        m.entry(pk).or_default().push(batch.project(&proj)?);
    }
    Ok(m)
}
