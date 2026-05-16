// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[allow(unused_imports)]
use super::error::StateEngineError;
use super::metrics::StateMetricsCollector;
use super::operator_state::{MemTable, OperatorStateStore, TombstoneMap};
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub struct SpillJob {
    pub store: Arc<OperatorStateStore>,
    pub epoch: u64,
    pub data: MemTable,
    pub tombstone_snapshot: TombstoneMap,
}

pub enum CompactJob {
    Minor { store: Arc<OperatorStateStore> },
    Major { store: Arc<OperatorStateStore> },
}

pub struct IoPool {
    spill_tx: Option<Sender<SpillJob>>,
    compact_tx: Option<Sender<CompactJob>>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl IoPool {
    pub fn try_new(
        spill_threads: usize,
        compact_threads: usize,
        metrics: Arc<dyn StateMetricsCollector>,
    ) -> std::io::Result<(Self, IoManager)> {
        let (spill_tx, spill_rx) = bounded::<SpillJob>(1024);
        let (compact_tx, compact_rx) = bounded::<CompactJob>(256);
        let mut worker_handles = Vec::with_capacity(spill_threads + compact_threads);

        for i in 0..spill_threads.max(1) {
            let rx = spill_rx.clone();
            let m = metrics.clone();
            let handle = thread::Builder::new()
                .name(format!("fs-spill-worker-{i}"))
                .spawn(move || spill_worker_loop(rx, m))?;
            worker_handles.push(handle);
        }

        for i in 0..compact_threads.max(1) {
            let rx = compact_rx.clone();
            let m = metrics.clone();
            let handle = thread::Builder::new()
                .name(format!("fs-compact-worker-{i}"))
                .spawn(move || compact_worker_loop(rx, m))?;
            worker_handles.push(handle);
        }

        let manager = IoManager {
            spill_tx: spill_tx.clone(),
            compact_tx: compact_tx.clone(),
        };

        Ok((
            Self {
                spill_tx: Some(spill_tx),
                compact_tx: Some(compact_tx),
                worker_handles,
            },
            manager,
        ))
    }

    pub fn shutdown(mut self) {
        tracing::info!("Initiating graceful shutdown for IoPool...");
        self.spill_tx.take();
        self.compact_tx.take();
        for handle in self.worker_handles.drain(..) {
            if let Err(e) = handle.join() {
                tracing::error!("I/O Worker thread panicked during shutdown: {:?}", e);
            }
        }
        tracing::info!("IoPool graceful shutdown completed.");
    }
}

#[derive(Clone)]
pub struct IoManager {
    spill_tx: Sender<SpillJob>,
    compact_tx: Sender<CompactJob>,
}

impl IoManager {
    pub fn try_send_spill(&self, job: SpillJob) -> Result<(), TrySendError<SpillJob>> {
        self.spill_tx.try_send(job)
    }
    pub fn try_send_compact(&self, job: CompactJob) -> Result<(), TrySendError<CompactJob>> {
        self.compact_tx.try_send(job)
    }
    pub fn pending_spills(&self) -> usize {
        self.spill_tx.len()
    }
}

fn spill_worker_loop(rx: Receiver<SpillJob>, metrics: Arc<dyn StateMetricsCollector>) {
    while let Ok(job) = rx.recv() {
        let op_id = job.store.operator_id;
        let epoch = job.epoch;
        let start = Instant::now();

        let result = catch_unwind(AssertUnwindSafe(|| {
            job.store
                .execute_spill_sync(job.epoch, job.data, job.tombstone_snapshot, &metrics)
        }));

        let duration_ms = start.elapsed().as_millis();
        metrics.record_spill_duration(op_id, duration_ms);

        match result {
            Ok(Ok(())) => tracing::debug!(op_id, epoch, duration_ms, "Spill success"),
            Ok(Err(e)) => tracing::error!(op_id, epoch, duration_ms, %e, "Spill I/O Error"),
            Err(_) => tracing::error!(op_id, epoch, "CRITICAL: Spill thread PANICKED! Recovered."),
        }
    }
}

fn compact_worker_loop(rx: Receiver<CompactJob>, metrics: Arc<dyn StateMetricsCollector>) {
    while let Ok(job) = rx.recv() {
        let (store, is_major) = match job {
            CompactJob::Minor { store } => (store, false),
            CompactJob::Major { store } => (store, true),
        };

        let op_id = store.operator_id;
        let start = Instant::now();

        let result = catch_unwind(AssertUnwindSafe(|| {
            store.execute_compact_sync(is_major, &metrics)
        }));

        let duration_ms = start.elapsed().as_millis();
        metrics.record_compaction_duration(op_id, is_major, duration_ms);

        match result {
            Ok(Ok(())) => tracing::info!(op_id, is_major, duration_ms, "Compaction success"),
            Ok(Err(e)) => tracing::error!(op_id, is_major, duration_ms, %e, "Compaction I/O Error"),
            Err(_) => tracing::error!(op_id, is_major, "CRITICAL: Compact thread PANICKED!"),
        }
    }
}
