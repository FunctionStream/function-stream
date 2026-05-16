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
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use protocol::function_stream_graph::{ChainedOperator, FsProgram};
use protocol::storage::{SourceCheckpointInfo, source_checkpoint_info};

use crate::config::{
    DEFAULT_CHECKPOINT_INTERVAL_MS, DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES,
    DEFAULT_PIPELINE_PARALLELISM,
};
use crate::memory::global_memory_pool;
use crate::sql::logical_node::logical::OperatorName;
use crate::stream_catalog::CatalogManager;
use crate::streaming::api::context::TaskContext;
use crate::streaming::api::operator::{ConstructedOperator, Operator};
use crate::streaming::api::source::SourceOperator;
use crate::streaming::execution::{ChainBuilder, Pipeline, SourceDriver};
use crate::streaming::factory::OperatorFactory;
use crate::streaming::job::edge_manager::EdgeManager;
use crate::streaming::job::models::{
    PhysicalExecutionGraph, PhysicalPipeline, PipelineStatus, StreamingJobRollupStatus,
};
use crate::streaming::network::endpoint::{BoxedEventStream, PhysicalSender};
use crate::streaming::protocol::control::{ControlCommand, JobMasterEvent, StopMode};
use crate::streaming::protocol::event::CheckpointBarrier;
use crate::streaming::state::{IoManager, IoPool, NoopMetricsCollector};

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
    pub job_manager_control_plane_threads: u32,
    pub job_manager_data_plane_threads: u32,
    /// Total bytes shared by all [`crate::streaming::state::OperatorStateStore`] (global pool).
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
            job_manager_control_plane_threads: 2,
            job_manager_data_plane_threads: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1),
            per_operator_memory_bytes: DEFAULT_OPERATOR_STATE_STORE_MEMORY_BYTES,
        }
    }
}

static GLOBAL_JOB_MANAGER: OnceLock<Arc<JobManager>> = OnceLock::new();

/// Operators that create an [`crate::streaming::state::OperatorStateStore`] at runtime.
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
    control_rt: Arc<tokio::runtime::Runtime>,
    data_rt: Arc<tokio::runtime::Runtime>,
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
    source_control_txs: Vec<UnboundedSender<ControlCommand>>,
    all_pipeline_control_txs: Vec<UnboundedSender<ControlCommand>>,
    job_master_rx: mpsc::Receiver<JobMasterEvent>,
    expected_pipeline_ids: HashSet<u32>,
    interval_ms: u64,
    start_epoch: u64,
    timeout: Duration,
}

impl PipelineRunner {
    async fn run(self) -> Result<(), crate::streaming::error::RunError> {
        match self {
            PipelineRunner::Source(driver) => driver.run().await,
            PipelineRunner::Standard(pipeline) => pipeline.run().await,
        }
    }
}

impl JobManager {
    pub fn new(
        operator_factory: Arc<OperatorFactory>,
        state_base_dir: impl AsRef<Path>,
        state_config: StateConfig,
    ) -> Result<Self> {
        let control_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(state_config.job_manager_control_plane_threads.max(1) as usize)
            .thread_name("fs-control-plane")
            .enable_all()
            .build()
            .context("Failed to initialize control runtime")?;
        let data_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(state_config.job_manager_data_plane_threads.max(1) as usize)
            .thread_name("fs-data-plane")
            .enable_all()
            .build()
            .context("Failed to initialize data runtime")?;
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
            control_rt: Arc::new(control_rt),
            data_rt: Arc::new(data_rt),
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

    /// Per-job state directory (source offset snapshots, operator state roots, etc.).
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
        source_checkpoint_infos: Vec<SourceCheckpointInfo>,
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
                    &source_checkpoint_infos,
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
            timeout: Duration::from_millis(interval_ms.max(1) * 3),
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
            let _ = tx.send(ControlCommand::Stop { mode: mode.clone() });
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
    fn extract_control_senders(
        &self,
        job_id: &str,
    ) -> Result<Vec<mpsc::UnboundedSender<ControlCommand>>> {
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
        source_checkpoint_infos: &[SourceCheckpointInfo],
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

        let (control_tx, control_rx) = mpsc::unbounded_channel();
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

        let runner = if let Some(mut source) = chain.source {
            // Filter checkpoint records for this pipeline and inject into the source operator
            // so it can restore partition offsets in on_start without touching TaskContext.
            let pipeline_checkpoint_infos: Vec<SourceCheckpointInfo> = source_checkpoint_infos
                .iter()
                .filter(|info| match &info.info {
                    Some(source_checkpoint_info::Info::Kafka(cp)) => cp.pipeline_id == pipeline_id,
                    None => false,
                })
                .cloned()
                .collect();
            if !pipeline_checkpoint_infos.is_empty() {
                source.set_recovery_checkpoint(pipeline_checkpoint_infos);
            }
            let chain_head = ChainBuilder::build(chain.operators);
            PipelineRunner::Source(SourceDriver::new(source, chain_head, ctx, control_rx))
        } else {
            PipelineRunner::Standard(
                Pipeline::new(chain.operators, ctx, physical_inboxes, control_rx).with_context(
                    || format!("Failed to initialize Standard Pipeline {}", pipeline_id),
                )?,
            )
        };

        let handle = self.spawn_worker_task(job_id, pipeline_id, runner, Arc::clone(&status));

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

    fn spawn_worker_task(
        &self,
        job_id: String,
        pipeline_id: u32,
        runner: PipelineRunner,
        status: Arc<RwLock<PipelineStatus>>,
    ) -> TokioJoinHandle<()> {
        self.data_rt.spawn(async move {
            if let Ok(mut st) = status.write() {
                *st = PipelineStatus::Running;
            }

            let execution_result = runner
                .run()
                .await
                .map_err(|e| anyhow!("Execution failed: {e}"));

            Self::handle_pipeline_exit(&job_id, pipeline_id, execution_result, &status);
        })
    }

    fn handle_pipeline_exit(
        job_id: &str,
        pipeline_id: u32,
        result: Result<()>,
        status: &RwLock<PipelineStatus>,
    ) {
        let (final_status, is_fatal) = match result {
            Ok(_) => {
                info!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline finished gracefully.");
                (PipelineStatus::Finished, false)
            }
            Err(e) => {
                error!(job_id = %job_id, pipeline_id = pipeline_id, error = %e, "Pipeline failed.");
                (
                    PipelineStatus::Failed {
                        error: e.to_string(),
                        is_panic: false,
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
        self.control_rt.spawn(async move {
            let CheckpointCoordinatorConfig {
                job_id,
                mut source_control_txs,
                all_pipeline_control_txs,
                mut job_master_rx,
                expected_pipeline_ids,
                interval_ms,
                start_epoch,
                timeout,
            } = cfg;
            if interval_ms == 0 {
                info!(job_id = %job_id, "Checkpoint disabled for this job");
                return;
            }

            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            interval.tick().await;

            let mut current_epoch: u64 = start_epoch;
            struct PendingCheckpoint {
                epoch: u64,
                missing_acks: HashSet<u32>,
                start_time: Instant,
                source_infos: Vec<SourceCheckpointInfo>,
            }
            let mut active_checkpoint: Option<PendingCheckpoint> = None;

            let broadcast_cmd = |cmd: ControlCommand| {
                for tx in &all_pipeline_control_txs {
                    let _ = tx.send(cmd.clone());
                }
            };

            loop {
                tokio::select! {
                    biased;

                    Some(event) = job_master_rx.recv() => {
                        match event {
                            JobMasterEvent::CheckpointAck {
                                pipeline_id,
                                epoch,
                                source_infos,
                            } => {
                                if let Some(pending) = &mut active_checkpoint {
                                    if pending.epoch != epoch {
                                        continue;
                                    }
                                    pending.missing_acks.remove(&pipeline_id);
                                    if !source_infos.is_empty() {
                                        pending.source_infos.extend(source_infos);
                                    }

                                    if pending.missing_acks.is_empty() {
                                        info!(
                                            job_id = %job_id, epoch = epoch,
                                            "Checkpoint Epoch is GLOBALLY COMPLETED (phase 1); persisting metadata and notifying operators (phase 2)"
                                        );

                                        let completed = active_checkpoint.take().expect("active checkpoint exists");

                                        let mut catalog_ok = true;
                                        if let Some(catalog) = CatalogManager::try_global() {
                                            if let Err(e) = catalog.commit_job_checkpoint(
                                                &job_id,
                                                epoch,
                                                completed.source_infos,
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
                                            ControlCommand::Commit { epoch }
                                        } else {
                                            ControlCommand::AbortCheckpoint { epoch }
                                        };
                                        broadcast_cmd(phase2);
                                    }
                                }
                            }
                            JobMasterEvent::CheckpointDecline { pipeline_id, epoch, reason } => {
                                if let Some(pending) = &active_checkpoint
                                    && pending.epoch == epoch
                                {
                                    error!(
                                        job_id = %job_id, epoch = epoch, pipeline_id = pipeline_id,
                                        reason = %reason, "Checkpoint FAILED!"
                                    );
                                    broadcast_cmd(ControlCommand::AbortCheckpoint { epoch });
                                    active_checkpoint = None;
                                }
                            }
                        }
                    }

                    _ = interval.tick() => {
                        if let Some(pending) = &active_checkpoint {
                            if pending.start_time.elapsed() > timeout {
                                warn!(
                                    job_id = %job_id,
                                    epoch = pending.epoch,
                                    "Checkpoint timed out; aborting active epoch"
                                );
                                broadcast_cmd(ControlCommand::AbortCheckpoint {
                                    epoch: pending.epoch,
                                });
                            } else {
                                continue;
                            }
                        }

                        source_control_txs.retain(|tx| !tx.is_closed());
                        if source_control_txs.is_empty() {
                            info!(job_id = %job_id, "All source pipelines closed; checkpoint coordinator exiting");
                            break;
                        }

                        info!(job_id = %job_id, epoch = current_epoch, "Triggering global Checkpoint Barrier.");

                        let barrier = CheckpointBarrier {
                            epoch: current_epoch,
                            min_epoch: 0,
                            timestamp: std::time::SystemTime::now(),
                            then_stop: false,
                        };
                        active_checkpoint = Some(PendingCheckpoint {
                            epoch: current_epoch,
                            missing_acks: expected_pipeline_ids.clone(),
                            start_time: Instant::now(),
                            source_infos: Vec::new(),
                        });

                        for tx in &source_control_txs {
                            let cmd = ControlCommand::trigger_checkpoint(barrier);
                            let _ = tx.send(cmd);
                        }
                        current_epoch += 1;
                    }
                }
            }
        })
    }
}
