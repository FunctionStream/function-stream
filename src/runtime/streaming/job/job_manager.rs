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

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use tokio::sync::mpsc;
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use protocol::function_stream_graph::{ChainedOperator, FsProgram};
use protocol::storage::{
    KafkaSourceSubtaskCheckpoint, SourceCheckpointPayload, source_checkpoint_payload,
};

use crate::config::{
    DEFAULT_CHECKPOINT_INTERVAL_MS, DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES,
    DEFAULT_PIPELINE_PARALLELISM,
};
use crate::runtime::memory::global_memory_pool;
use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{ConstructedOperator, Operator};
use crate::runtime::streaming::api::source::SourceOperator;
use crate::runtime::streaming::execution::{ChainBuilder, Pipeline, SourceDriver};
use crate::runtime::streaming::factory::OperatorFactory;
use crate::runtime::streaming::job::edge_manager::EdgeManager;
use crate::runtime::streaming::job::models::{
    PhysicalExecutionGraph, PhysicalPipeline, PipelineStatus, StreamingJobRollupStatus,
};
use crate::runtime::streaming::network::endpoint::{BoxedEventStream, PhysicalSender};
use crate::runtime::streaming::protocol::control::{ControlCommand, JobMasterEvent, StopMode};
use crate::runtime::streaming::protocol::event::CheckpointBarrier;
use crate::runtime::streaming::state::{IoManager, IoPool, NoopMetricsCollector};
use crate::sql::logical_node::logical::OperatorName;
use crate::storage::stream_catalog::CatalogManager;

#[derive(Debug, Clone)]
pub struct StreamingJobSummary {
    pub job_id: String,
    pub status: StreamingJobRollupStatus,
    pub pipeline_count: i32,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone)]
pub struct PipelineDetail {
    pub pipeline_id: u32,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct StreamingJobDetail {
    pub job_id: String,
    pub status: StreamingJobRollupStatus,
    pub pipeline_count: i32,
    pub uptime_secs: u64,
    pub pipelines: Vec<PipelineDetail>,
    pub program: FsProgram,
}

#[derive(Debug, Clone)]
pub struct StateConfig {
    pub max_background_spills: usize,
    pub max_background_compactions: usize,
    pub soft_limit_ratio: f64,
    pub checkpoint_interval_ms: u64,
    pub pipeline_parallelism: u32,
    /// Total bytes shared by all [`crate::runtime::streaming::state::OperatorStateStore`] (global pool).
    pub per_operator_memory_bytes: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            max_background_spills: 4,
            max_background_compactions: 2,
            soft_limit_ratio: 0.7,
            checkpoint_interval_ms: DEFAULT_CHECKPOINT_INTERVAL_MS,
            pipeline_parallelism: DEFAULT_PIPELINE_PARALLELISM,
            per_operator_memory_bytes: DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES,
        }
    }
}

static GLOBAL_JOB_MANAGER: OnceLock<Arc<JobManager>> = OnceLock::new();

/// Operators that create an [`crate::runtime::streaming::state::OperatorStateStore`] at runtime.
fn pipeline_state_store_operator_count(operators: &[ChainedOperator]) -> usize {
    operators
        .iter()
        .filter(|op| {
            OperatorName::from_str(op.operator_name.as_str())
                .ok()
                .is_some_and(|n| {
                    matches!(
                        n,
                        OperatorName::Join
                            | OperatorName::InstantJoin
                            | OperatorName::WindowFunction
                            | OperatorName::TumblingWindowAggregate
                            | OperatorName::SlidingWindowAggregate
                            | OperatorName::SessionWindowAggregate
                            | OperatorName::UpdatingAggregate
                    )
                })
        })
        .count()
}

pub struct JobManager {
    active_jobs: Arc<RwLock<HashMap<String, PhysicalExecutionGraph>>>,
    operator_factory: Arc<OperatorFactory>,
    io_manager_client: IoManager,
    io_pool: Mutex<Option<IoPool>>,
    state_base_dir: PathBuf,
    state_config: StateConfig,
}

struct PreparedChain {
    source: Option<Box<dyn SourceOperator>>,
    operators: Vec<Box<dyn Operator>>,
}

enum PipelineRunner {
    Source(SourceDriver),
    Standard(Pipeline),
}

struct CheckpointCoordinatorConfig {
    job_id: String,
    source_control_txs: Vec<mpsc::Sender<ControlCommand>>,
    all_pipeline_control_txs: Vec<mpsc::Sender<ControlCommand>>,
    job_master_rx: mpsc::Receiver<JobMasterEvent>,
    expected_pipeline_ids: HashSet<u32>,
    interval_ms: u64,
    start_epoch: u64,
    job_state_dir: PathBuf,
}

impl PipelineRunner {
    async fn run(self) -> Result<(), crate::runtime::streaming::error::RunError> {
        match self {
            PipelineRunner::Source(driver) => driver.run().await,
            PipelineRunner::Standard(pipeline) => pipeline.run().await,
        }
    }
}

fn decode_kafka_checkpoints_from_source_payloads(
    payloads: Vec<SourceCheckpointPayload>,
    epoch: u64,
) -> Vec<KafkaSourceSubtaskCheckpoint> {
    let mut out = Vec::new();
    for p in payloads {
        match p.checkpoint {
            Some(source_checkpoint_payload::Checkpoint::Kafka(mut cp)) => {
                if cp.checkpoint_epoch != epoch {
                    cp.checkpoint_epoch = epoch;
                }
                out.push(cp);
            }
            None => warn!("Skip empty source checkpoint payload"),
        }
    }
    out
}

impl JobManager {
    pub fn new(
        operator_factory: Arc<OperatorFactory>,
        state_base_dir: impl AsRef<Path>,
        state_config: StateConfig,
    ) -> Result<Self> {
        let metrics = Arc::new(NoopMetricsCollector);
        let (io_pool, io_manager_client) = IoPool::try_new(
            state_config.max_background_spills,
            state_config.max_background_compactions,
            metrics,
        )
        .context("Failed to initialize state engine I/O pool")?;

        Ok(Self {
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            operator_factory,
            io_manager_client,
            io_pool: Mutex::new(Some(io_pool)),
            state_base_dir: state_base_dir.as_ref().to_path_buf(),
            state_config,
        })
    }

    pub fn init(
        factory: Arc<OperatorFactory>,
        state_base_dir: PathBuf,
        state_config: StateConfig,
    ) -> Result<()> {
        GLOBAL_JOB_MANAGER
            .set(Arc::new(Self::new(factory, state_base_dir, state_config)?))
            .map_err(|_| anyhow!("JobManager singleton already initialized"))
    }

    pub fn global() -> Result<Arc<Self>> {
        GLOBAL_JOB_MANAGER
            .get()
            .cloned()
            .ok_or_else(|| anyhow!("JobManager not initialized. Call init() first."))
    }

    pub fn shutdown(&self) {
        if let Some(pool) = self.io_pool.lock().unwrap().take() {
            pool.shutdown();
        }
    }

    #[inline]
    pub fn default_pipeline_parallelism(&self) -> u32 {
        self.state_config.pipeline_parallelism
    }

    /// Per-job state directory (Kafka offset snapshots, operator state roots, etc.).
    #[inline]
    pub fn job_state_directory(&self, job_id: &str) -> PathBuf {
        self.state_base_dir.join(job_id)
    }

    pub async fn submit_job(
        &self,
        job_id: String,
        program: FsProgram,
        custom_checkpoint_interval_ms: Option<u64>,
        recovery_epoch: Option<u64>,
    ) -> Result<String> {
        let mut edge_manager = EdgeManager::build(&program.nodes, &program.edges);
        let mut pipelines = HashMap::with_capacity(program.nodes.len());

        let mut source_control_txs = Vec::new();
        let mut all_pipeline_control_txs = Vec::new();
        let mut expected_pipeline_ids = HashSet::new();

        let job_state_dir = self.state_base_dir.join(&job_id);
        std::fs::create_dir_all(&job_state_dir).context("Failed to create job state dir")?;

        let (job_master_tx, job_master_rx) = mpsc::channel(256);

        let safe_epoch = recovery_epoch.unwrap_or(0);

        for node in &program.nodes {
            let pipeline_id = node.node_index as u32;

            let (pipeline, is_source) = self
                .build_and_spawn_pipeline(
                    job_id.clone(),
                    pipeline_id,
                    &node.operators,
                    node.parallelism,
                    &mut edge_manager,
                    &job_state_dir,
                    job_master_tx.clone(),
                    safe_epoch,
                )
                .with_context(|| {
                    format!(
                        "Failed to build pipeline {} for job {}",
                        pipeline_id, job_id
                    )
                })?;

            if is_source {
                source_control_txs.push(pipeline.control_tx.clone());
            }
            all_pipeline_control_txs.push(pipeline.control_tx.clone());
            expected_pipeline_ids.insert(pipeline_id);
            pipelines.insert(pipeline_id, pipeline);
        }

        let interval_ms =
            custom_checkpoint_interval_ms.unwrap_or(self.state_config.checkpoint_interval_ms);

        self.spawn_checkpoint_coordinator(CheckpointCoordinatorConfig {
            job_id: job_id.clone(),
            source_control_txs,
            all_pipeline_control_txs,
            job_master_rx,
            expected_pipeline_ids,
            interval_ms,
            start_epoch: safe_epoch + 1,
            job_state_dir: job_state_dir.clone(),
        });

        let graph = PhysicalExecutionGraph {
            job_id: job_id.clone(),
            program,
            pipelines,
            start_time: std::time::Instant::now(),
        };

        let mut jobs_guard = self
            .active_jobs
            .write()
            .map_err(|e| anyhow!("Active jobs lock poisoned: {}", e))?;
        jobs_guard.insert(job_id.clone(), graph);

        info!(job_id = %job_id, interval_ms, recovery_epoch = safe_epoch, "Job submitted successfully.");
        Ok(job_id)
    }

    pub async fn stop_job(&self, job_id: &str, mode: StopMode) -> Result<()> {
        let control_senders = self.extract_control_senders(job_id)?;

        for tx in control_senders {
            let _ = tx.send(ControlCommand::Stop { mode: mode.clone() }).await;
        }

        info!(job_id = %job_id, mode = ?mode, "Job stop signal dispatched.");
        Ok(())
    }

    pub async fn remove_job(&self, job_id: &str, mode: StopMode) -> Result<()> {
        self.stop_job(job_id, mode).await?;

        let mut jobs_guard = self
            .active_jobs
            .write()
            .map_err(|_| anyhow!("Active jobs lock poisoned"))?;

        if jobs_guard.remove(job_id).is_some() {
            info!(job_id = %job_id, "Job removed from JobManager.");
            Ok(())
        } else {
            bail!("Job not found during removal: {}", job_id)
        }
    }

    pub fn has_job(&self, job_id: &str) -> bool {
        self.active_jobs
            .read()
            .map(|guard| guard.contains_key(job_id))
            .unwrap_or(false)
    }

    pub fn list_jobs(&self) -> Vec<StreamingJobSummary> {
        let Ok(jobs_guard) = self.active_jobs.read() else {
            warn!("Failed to read active_jobs due to lock poisoning.");
            return vec![];
        };

        jobs_guard
            .values()
            .map(|graph| {
                let pipeline_count = graph.pipelines.len() as i32;
                let uptime_secs = graph.start_time.elapsed().as_secs();
                let status = Self::aggregate_pipeline_status(&graph.pipelines);
                StreamingJobSummary {
                    job_id: graph.job_id.clone(),
                    status,
                    pipeline_count,
                    uptime_secs,
                }
            })
            .collect()
    }

    pub fn get_job_detail(&self, job_id: &str) -> Option<StreamingJobDetail> {
        let jobs_guard = self.active_jobs.read().ok()?;
        let graph = jobs_guard.get(job_id)?;

        let uptime_secs = graph.start_time.elapsed().as_secs();
        let overall_status = Self::aggregate_pipeline_status(&graph.pipelines);

        let pipeline_details: Vec<PipelineDetail> = graph
            .pipelines
            .iter()
            .map(|(id, pipeline)| {
                let status = pipeline
                    .status
                    .read()
                    .map(|s| s.clone())
                    .unwrap_or_else(|_| PipelineStatus::Failed {
                        error: "Status lock poisoned".into(),
                        is_panic: true,
                    });

                PipelineDetail {
                    pipeline_id: *id,
                    status: format!("{status:?}"),
                }
            })
            .collect();

        Some(StreamingJobDetail {
            job_id: graph.job_id.clone(),
            status: overall_status,
            pipeline_count: graph.pipelines.len() as i32,
            uptime_secs,
            pipelines: pipeline_details,
            program: graph.program.clone(),
        })
    }

    pub fn get_pipeline_statuses(&self, job_id: &str) -> Option<HashMap<u32, PipelineStatus>> {
        let jobs_guard = self.active_jobs.read().ok()?;
        let graph = jobs_guard.get(job_id)?;

        Some(
            graph
                .pipelines
                .iter()
                .map(|(id, pipeline)| {
                    let status = pipeline
                        .status
                        .read()
                        .map(|s| s.clone())
                        .unwrap_or_else(|_| PipelineStatus::Failed {
                            error: "Status lock poisoned".into(),
                            is_panic: true,
                        });
                    (*id, status)
                })
                .collect(),
        )
    }

    fn aggregate_pipeline_status(
        pipelines: &HashMap<u32, PhysicalPipeline>,
    ) -> StreamingJobRollupStatus {
        let mut running = 0u32;
        let mut failed = 0u32;
        let mut finished = 0u32;
        let mut initializing = 0u32;

        for pipeline in pipelines.values() {
            let status = pipeline
                .status
                .read()
                .map(|s| s.clone())
                .unwrap_or_else(|_| PipelineStatus::Failed {
                    error: "Status lock poisoned".into(),
                    is_panic: true,
                });

            match status {
                PipelineStatus::Running => running += 1,
                PipelineStatus::Failed { .. } => failed += 1,
                PipelineStatus::Finished => finished += 1,
                PipelineStatus::Initializing => initializing += 1,
                PipelineStatus::Stopping => {}
            }
        }

        let n = pipelines.len() as u32;
        if failed > 0 {
            StreamingJobRollupStatus::Degraded
        } else if running > 0 && running == n {
            StreamingJobRollupStatus::Running
        } else if finished == n {
            StreamingJobRollupStatus::Finished
        } else if initializing > 0 {
            StreamingJobRollupStatus::Initializing
        } else {
            StreamingJobRollupStatus::Reconciling
        }
    }
    fn extract_control_senders(&self, job_id: &str) -> Result<Vec<mpsc::Sender<ControlCommand>>> {
        let jobs_guard = self
            .active_jobs
            .read()
            .map_err(|_| anyhow!("Active jobs lock poisoned"))?;

        let graph = jobs_guard
            .get(job_id)
            .ok_or_else(|| anyhow!("Job not found: {job_id}"))?;

        Ok(graph
            .pipelines
            .values()
            .map(|p| p.control_tx.clone())
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_and_spawn_pipeline(
        &self,
        job_id: String,
        pipeline_id: u32,
        operators: &[ChainedOperator],
        declared_parallelism: u32,
        edge_manager: &mut EdgeManager,
        job_state_dir: &Path,
        job_master_tx: mpsc::Sender<JobMasterEvent>,
        recovery_epoch: u64,
    ) -> Result<(PhysicalPipeline, bool)> {
        let (raw_inboxes, raw_outboxes) =
            edge_manager.take_endpoints(pipeline_id).with_context(|| {
                format!(
                    "Failed to retrieve network endpoints for pipeline {}",
                    pipeline_id
                )
            })?;

        let physical_outboxes: Vec<PhysicalSender> = raw_outboxes
            .into_iter()
            .map(PhysicalSender::Local)
            .collect();

        let physical_inboxes: Vec<BoxedEventStream> = raw_inboxes
            .into_iter()
            .map(|rx| Box::pin(ReceiverStream::new(rx)) as _)
            .collect();

        let chain = self.build_operator_chain(operators).with_context(|| {
            format!(
                "Failed to build operator chain for pipeline {}",
                pipeline_id
            )
        })?;

        let is_source = chain.source.is_some();

        ensure!(
            chain.source.is_some() || !physical_inboxes.is_empty(),
            "Topology Error: Pipeline '{}' contains no source and has no upstream inputs (Dead end).",
            pipeline_id
        );
        ensure!(
            chain.source.is_none() || physical_inboxes.is_empty(),
            "Topology Error: Source pipeline '{}' cannot have upstream inputs.",
            pipeline_id
        );

        let (control_tx, control_rx) = mpsc::channel(64);
        let status = Arc::new(RwLock::new(PipelineStatus::Initializing));

        let subtask_index = 0;
        let parallelism = if declared_parallelism > 0 {
            declared_parallelism
        } else {
            self.state_config.pipeline_parallelism
        }
        .max(1);

        let per_op = self.state_config.per_operator_memory_bytes;
        let n_state_ops = pipeline_state_store_operator_count(operators);
        let pipeline_state_memory_block = if n_state_ops > 0 {
            let bytes = per_op
                .checked_mul(n_state_ops as u64)
                .ok_or_else(|| anyhow!("pipeline state memory byte size overflow"))?;
            Some(
                global_memory_pool()
                    .try_request_block(bytes)
                    .map_err(|e| anyhow!("pipeline state memory reservation failed: {e}"))?,
            )
        } else {
            None
        };

        let ctx = TaskContext::new(
            job_id.clone(),
            pipeline_id,
            subtask_index,
            parallelism,
            physical_outboxes,
            Arc::clone(&global_memory_pool()),
            self.io_manager_client.clone(),
            job_state_dir.to_path_buf(),
            pipeline_state_memory_block,
            per_op,
            recovery_epoch,
            Some(job_master_tx.clone()),
        );

        let runner = if let Some(source) = chain.source {
            let chain_head = ChainBuilder::build(chain.operators);
            PipelineRunner::Source(SourceDriver::new(source, chain_head, ctx, control_rx))
        } else {
            PipelineRunner::Standard(
                Pipeline::new(chain.operators, ctx, physical_inboxes, control_rx).with_context(
                    || format!("Failed to initialize Standard Pipeline {}", pipeline_id),
                )?,
            )
        };

        let handle = self
            .spawn_worker_thread(job_id, pipeline_id, runner, Arc::clone(&status))
            .with_context(|| format!("Failed to spawn OS thread for pipeline {}", pipeline_id))?;

        let pipeline = PhysicalPipeline {
            pipeline_id,
            handle: Some(handle),
            status,
            control_tx,
        };
        Ok((pipeline, is_source))
    }

    fn build_operator_chain(&self, operator_configs: &[ChainedOperator]) -> Result<PreparedChain> {
        let mut source: Option<Box<dyn SourceOperator>> = None;
        let mut chain = Vec::with_capacity(operator_configs.len());

        for op_config in operator_configs {
            let constructed = self
                .operator_factory
                .create_operator(&op_config.operator_name, &op_config.operator_config)?;

            match constructed {
                ConstructedOperator::Operator(msg_op) => chain.push(msg_op),
                ConstructedOperator::Source(src_op) => {
                    if source.is_some() {
                        bail!("Topology Error: Multiple sources in one physical chain.");
                    }
                    if !chain.is_empty() {
                        bail!(
                            "Topology Error: Source '{}' must be the first operator.",
                            op_config.operator_name
                        );
                    }
                    source = Some(src_op);
                }
            }
        }
        Ok(PreparedChain {
            source,
            operators: chain,
        })
    }

    fn spawn_worker_thread(
        &self,
        job_id: String,
        pipeline_id: u32,
        runner: PipelineRunner,
        status: Arc<RwLock<PipelineStatus>>,
    ) -> Result<std::thread::JoinHandle<()>> {
        let thread_name = format!("Task-{job_id}-{pipeline_id}");

        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                if let Ok(mut st) = status.write() {
                    *st = PipelineStatus::Running;
                }

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build current-thread Tokio runtime");

                let execution_result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        rt.block_on(async move {
                            runner
                                .run()
                                .await
                                .map_err(|e| anyhow!("Execution failed: {e}"))
                        })
                    }));

                Self::handle_pipeline_exit(&job_id, pipeline_id, execution_result, &status);
            })?;

        Ok(handle)
    }

    fn handle_pipeline_exit(
        job_id: &str,
        pipeline_id: u32,
        thread_result: std::thread::Result<Result<()>>,
        status: &RwLock<PipelineStatus>,
    ) {
        let (final_status, is_fatal) = match thread_result {
            Ok(Ok(_)) => {
                info!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline finished gracefully.");
                (PipelineStatus::Finished, false)
            }
            Ok(Err(e)) => {
                error!(job_id = %job_id, pipeline_id = pipeline_id, error = %e, "Pipeline failed.");
                (
                    PipelineStatus::Failed {
                        error: e.to_string(),
                        is_panic: false,
                    },
                    true,
                )
            }
            Err(_) => {
                error!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline thread panicked!");
                (
                    PipelineStatus::Failed {
                        error: "Unexpected panic in task thread".into(),
                        is_panic: true,
                    },
                    true,
                )
            }
        };

        if let Ok(mut st) = status.write() {
            *st = final_status;
        }

        if is_fatal {
            warn!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline failure detected. Job degraded.");
        }
    }

    // ========================================================================
    // Chandy-Lamport distributed snapshot barrier coordinator
    // ========================================================================

    fn spawn_checkpoint_coordinator(
        &self,
        cfg: CheckpointCoordinatorConfig,
    ) -> TokioJoinHandle<()> {
        let CheckpointCoordinatorConfig {
            job_id,
            source_control_txs,
            all_pipeline_control_txs,
            mut job_master_rx,
            expected_pipeline_ids,
            interval_ms,
            start_epoch,
            job_state_dir,
        } = cfg;
        tokio::spawn(async move {
            if interval_ms == 0 {
                info!(job_id = %job_id, "Checkpoint disabled for this job");
                return;
            }

            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            interval.tick().await;

            let mut current_epoch: u64 = start_epoch;
            let mut pending_checkpoints: HashMap<u64, HashSet<u32>> = HashMap::new();
            let mut source_reports: HashMap<u64, Vec<SourceCheckpointPayload>> = HashMap::new();

            async fn broadcast_checkpoint_phase2(
                txs: &[mpsc::Sender<ControlCommand>],
                cmd: ControlCommand,
            ) {
                for tx in txs {
                    let _ = tx.send(cmd.clone()).await;
                }
            }

            loop {
                tokio::select! {
                    biased;

                    Some(event) = job_master_rx.recv() => {
                        match event {
                            JobMasterEvent::CheckpointAck {
                                pipeline_id,
                                epoch,
                                source_payloads,
                            } => {
                                if !source_payloads.is_empty() {
                                    source_reports
                                        .entry(epoch)
                                        .or_default()
                                        .extend(source_payloads);
                                }
                                if let Some(pending_set) = pending_checkpoints.get_mut(&epoch) {
                                    pending_set.remove(&pipeline_id);

                                    if pending_set.is_empty() {
                                        info!(
                                            job_id = %job_id, epoch = epoch,
                                            "Checkpoint Epoch is GLOBALLY COMPLETED (phase 1); persisting metadata and notifying operators (phase 2)"
                                        );

                                        let payloads = source_reports.remove(&epoch).unwrap_or_default();
                                        let kf = decode_kafka_checkpoints_from_source_payloads(payloads, epoch);
                                        let epoch_u32 = u32::try_from(epoch).unwrap_or(u32::MAX);

                                        let mut catalog_ok = true;
                                        if let Some(catalog) = CatalogManager::try_global() {
                                            if let Err(e) = catalog.commit_job_checkpoint(
                                                &job_id,
                                                epoch,
                                                &job_state_dir,
                                                kf,
                                            ) {
                                                catalog_ok = false;
                                                error!(
                                                    job_id = %job_id, epoch = epoch,
                                                    error = %e,
                                                    "Failed to commit checkpoint metadata to Catalog — aborting transactional sinks"
                                                );
                                            }
                                        } else {
                                            warn!(
                                                job_id = %job_id, epoch = epoch,
                                                "CatalogManager not available; proceeding with operator Commit (Kafka transactional commit) only"
                                            );
                                        }

                                        let phase2 = if catalog_ok {
                                            ControlCommand::Commit { epoch: epoch_u32 }
                                        } else {
                                            ControlCommand::AbortCheckpoint { epoch: epoch_u32 }
                                        };
                                        broadcast_checkpoint_phase2(&all_pipeline_control_txs, phase2).await;

                                        pending_checkpoints.remove(&epoch);
                                    }
                                }
                            }
                            JobMasterEvent::CheckpointDecline { pipeline_id, epoch, reason } => {
                                error!(
                                    job_id = %job_id, epoch = epoch, pipeline_id = pipeline_id,
                                    reason = %reason, "Checkpoint FAILED!"
                                );
                                if pending_checkpoints.remove(&epoch).is_some() {
                                    source_reports.remove(&epoch);
                                    let epoch_u32 = u32::try_from(epoch).unwrap_or(u32::MAX);
                                    broadcast_checkpoint_phase2(
                                        &all_pipeline_control_txs,
                                        ControlCommand::AbortCheckpoint { epoch: epoch_u32 },
                                    )
                                    .await;
                                }
                            }
                        }
                    }

                    _ = interval.tick(), if pending_checkpoints.is_empty() => {
                        info!(job_id = %job_id, epoch = current_epoch, "Triggering global Checkpoint Barrier.");
                        pending_checkpoints.insert(current_epoch, expected_pipeline_ids.clone());

                        let barrier = CheckpointBarrier {
                            epoch: current_epoch as u32,
                            min_epoch: 0,
                            timestamp: std::time::SystemTime::now(),
                            then_stop: false,
                        };

                        for tx in &source_control_txs {
                            let cmd = ControlCommand::trigger_checkpoint(barrier);
                            if tx.send(cmd).await.is_err() {
                                debug!(job_id = %job_id, "Source disconnected. Shutting down coordinator.");
                                return;
                            }
                        }
                        current_epoch += 1;
                    }
                }
            }
        })
    }
}
