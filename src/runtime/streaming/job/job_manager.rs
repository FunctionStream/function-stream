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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use protocol::grpc::api::{ChainedOperator, FsProgram};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{ConstructedOperator, Operator};
use crate::runtime::streaming::api::source::SourceOperator;
use crate::runtime::streaming::execution::{ChainBuilder, Pipeline, SourceDriver};
use crate::runtime::streaming::factory::OperatorFactory;
use crate::runtime::streaming::job::edge_manager::EdgeManager;
use crate::runtime::streaming::job::models::{
    PhysicalExecutionGraph, PhysicalPipeline, PipelineStatus, StreamingJobRollupStatus,
};
use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::network::endpoint::{BoxedEventStream, PhysicalSender};
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};

// ---------------------------------------------------------------------------
// 核心数据结构
// ---------------------------------------------------------------------------
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

static GLOBAL_JOB_MANAGER: OnceLock<Arc<JobManager>> = OnceLock::new();

pub struct JobManager {
    active_jobs: Arc<RwLock<HashMap<String, PhysicalExecutionGraph>>>,
    operator_factory: Arc<OperatorFactory>,
    memory_pool: Arc<MemoryPool>,
}

struct PreparedChain {
    source: Option<Box<dyn SourceOperator>>,
    operators: Vec<Box<dyn Operator>>,
}

enum PipelineRunner {
    Source(SourceDriver),
    Standard(Pipeline),
}

impl PipelineRunner {
    async fn run(self) -> Result<(), crate::runtime::streaming::error::RunError> {
        match self {
            PipelineRunner::Source(driver) => driver.run().await,
            PipelineRunner::Standard(pipeline) => pipeline.run().await,
        }
    }
}

impl JobManager {
    pub fn new(operator_factory: Arc<OperatorFactory>, max_memory_bytes: usize) -> Self {
        Self {
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            operator_factory,
            memory_pool: MemoryPool::new(max_memory_bytes),
        }
    }

    pub fn init(factory: Arc<OperatorFactory>, memory_bytes: usize) -> Result<()> {
        GLOBAL_JOB_MANAGER
            .set(Arc::new(Self::new(factory, memory_bytes)))
            .map_err(|_| anyhow!("JobManager singleton already initialized"))
    }

    pub fn global() -> Result<Arc<Self>> {
        GLOBAL_JOB_MANAGER
            .get()
            .cloned()
            .ok_or_else(|| anyhow!("JobManager not initialized. Call init() first."))
    }

    pub async fn submit_job(&self, job_id: String, program: FsProgram) -> Result<String> {
        let mut edge_manager = EdgeManager::build(&program.nodes, &program.edges);
        let mut pipelines = HashMap::with_capacity(program.nodes.len());

        for node in &program.nodes {
            let pipeline_id = node.node_index as u32;

            let pipeline = self
                .build_and_spawn_pipeline(
                    job_id.clone(),
                    pipeline_id,
                    &node.operators,
                    &mut edge_manager,
                )
                .with_context(|| {
                    format!(
                        "Failed to build pipeline {} for job {}",
                        pipeline_id, job_id
                    )
                })?;

            pipelines.insert(pipeline_id, pipeline);
        }

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

        info!(job_id = %job_id, "Job submitted successfully.");
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

    fn build_and_spawn_pipeline(
        &self,
        job_id: String,
        pipeline_id: u32,
        operators: &[ChainedOperator],
        edge_manager: &mut EdgeManager,
    ) -> Result<PhysicalPipeline> {
        let (raw_inboxes, raw_outboxes) = edge_manager.take_endpoints(pipeline_id);

        let physical_outboxes = raw_outboxes
            .into_iter()
            .map(PhysicalSender::Local)
            .collect();
        let physical_inboxes: Vec<BoxedEventStream> = raw_inboxes
            .into_iter()
            .map(|rx| Box::pin(ReceiverStream::new(rx)) as _)
            .collect();

        let chain = self.build_operator_chain(operators)?;

        if chain.source.is_none() && physical_inboxes.is_empty() {
            bail!(
                "Topology Error: pipeline '{}' contains no source and no upstream inputs.",
                pipeline_id
            );
        }
        if chain.source.is_some() && !physical_inboxes.is_empty() {
            bail!(
                "Topology Error: source pipeline '{}' should not have upstream inputs.",
                pipeline_id
            );
        }

        let (control_tx, control_rx) = mpsc::channel(64);
        let status = Arc::new(RwLock::new(PipelineStatus::Initializing));

        let ctx = TaskContext::new(
            job_id.clone(),
            pipeline_id,
            0,
            1,
            physical_outboxes,
            Arc::clone(&self.memory_pool),
        );

        let runner = if let Some(source) = chain.source {
            let chain_head = ChainBuilder::build(chain.operators);
            PipelineRunner::Source(SourceDriver::new(source, chain_head, ctx, control_rx))
        } else {
            PipelineRunner::Standard(
                Pipeline::new(chain.operators, ctx, physical_inboxes, control_rx)
                    .map_err(|e| anyhow!("Pipeline init failed: {e}"))?,
            )
        };

        let handle = self.spawn_worker_thread(job_id, pipeline_id, runner, Arc::clone(&status))?;

        Ok(PhysicalPipeline {
            pipeline_id,
            handle: Some(handle),
            status,
            control_tx,
        })
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
}
