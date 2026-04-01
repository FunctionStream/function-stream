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

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use protocol::grpc::api::{ChainedOperator, FsProgram};

use crate::sql::common::render_program_topology;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{ConstructedOperator, Operator};
use crate::runtime::streaming::api::source::SourceOperator;
use crate::runtime::streaming::execution::runner::{ChainedDriver, Pipeline};
use crate::runtime::streaming::execution::source::SourceRunner;
use crate::runtime::streaming::factory::OperatorFactory;
use crate::runtime::streaming::job::edge_manager::EdgeManager;
use crate::runtime::streaming::job::models::{PhysicalExecutionGraph, PhysicalPipeline, PipelineStatus};
use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::network::endpoint::{BoxedEventStream, PhysicalSender};
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};

#[derive(Debug, Clone)]
pub struct StreamingJobSummary {
    pub job_id: String,
    pub status: String,
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
    pub status: String,
    pub pipeline_count: i32,
    pub uptime_secs: u64,
    pub pipelines: Vec<PipelineDetail>,
    pub topology: String,
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

impl JobManager {
    pub fn new(operator_factory: Arc<OperatorFactory>, max_memory_bytes: usize) -> Self {
        Self {
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            operator_factory,
            memory_pool: MemoryPool::new(max_memory_bytes),
        }
    }

    pub fn init(operator_factory: Arc<OperatorFactory>, max_memory_bytes: usize) -> anyhow::Result<()> {
        let manager = Arc::new(Self::new(operator_factory, max_memory_bytes));
        GLOBAL_JOB_MANAGER
            .set(manager)
            .map_err(|_| anyhow!("JobManager singleton already initialized"))
    }

    pub fn global() -> anyhow::Result<Arc<Self>> {
        GLOBAL_JOB_MANAGER
            .get()
            .cloned()
            .ok_or_else(|| anyhow!("JobManager not initialized. Call init() first."))
    }

    ///
    pub async fn submit_job(&self, job_id: String, program: FsProgram) -> anyhow::Result<String> {
        let mut edge_manager = EdgeManager::build(&program.nodes, &program.edges);
        let mut pipelines = HashMap::new();

        for node in &program.nodes {
            let pipeline_id = node.node_index as u32;

            let (raw_inboxes, raw_outboxes) = edge_manager.take_endpoints(pipeline_id);
            let physical_outboxes = raw_outboxes.into_iter().map(PhysicalSender::Local).collect();
            let physical_inboxes: Vec<BoxedEventStream> = raw_inboxes
                .into_iter()
                .map(|rx| Box::pin(ReceiverStream::new(rx)) as _)
                .collect();

            let chain = self.build_operator_chain(&node.operators)?;
            if chain.source.is_none() && physical_inboxes.is_empty() {
                anyhow::bail!(
                    "Topology Error: pipeline '{}' contains no source operator and has no upstream inputs.",
                    pipeline_id
                );
            }
            if chain.source.is_some() && !physical_inboxes.is_empty() {
                anyhow::bail!(
                    "Topology Error: source pipeline '{}' should not have upstream inputs.",
                    pipeline_id
                );
            }

            let (control_tx, control_rx) = mpsc::channel(64);
            let status = Arc::new(RwLock::new(PipelineStatus::Initializing));

            let handle = if let Some(source) = chain.source {
                self.spawn_source_pipeline_thread(
                    job_id.clone(),
                    pipeline_id,
                    source,
                    chain.operators,
                    physical_outboxes,
                    control_rx,
                    Arc::clone(&status),
                )?
            } else {
                self.spawn_pipeline_thread(
                    job_id.clone(),
                    pipeline_id,
                    chain.operators,
                    physical_inboxes,
                    physical_outboxes,
                    control_rx,
                    Arc::clone(&status),
                )?
            };

            pipelines.insert(
                pipeline_id,
                PhysicalPipeline {
                    pipeline_id,
                    handle: Some(handle),
                    status,
                    control_tx,
                },
            );
        }

        let graph = PhysicalExecutionGraph {
            job_id: job_id.clone(),
            program,
            pipelines,
            start_time: std::time::Instant::now(),
        };

        self.active_jobs.write().unwrap().insert(job_id.clone(), graph);
        info!(job_id = %job_id, "Job submitted successfully.");

        Ok(job_id)
    }

    pub async fn stop_job(&self, job_id: &str, mode: StopMode) -> anyhow::Result<()> {
        let control_senders: Vec<_> = {
            let jobs_guard = self.active_jobs.read().unwrap();
            let graph = jobs_guard
                .get(job_id)
                .ok_or_else(|| anyhow::anyhow!("Job not found: {job_id}"))?;

            graph.pipelines.values().map(|p| p.control_tx.clone()).collect()
        };

        for tx in control_senders {
            let _ = tx.send(ControlCommand::Stop { mode: mode.clone() }).await;
        }

        info!(job_id = %job_id, mode = ?mode, "Job stop signal dispatched.");
        Ok(())
    }

    pub fn get_pipeline_statuses(&self, job_id: &str) -> Option<HashMap<u32, PipelineStatus>> {
        let jobs_guard = self.active_jobs.read().unwrap();
        let graph = jobs_guard.get(job_id)?;

        Some(
            graph.pipelines
                .iter()
                .map(|(id, pipeline)| {
                    (*id, pipeline.status.read().unwrap().clone())
                })
                .collect(),
        )
    }

    pub fn list_jobs(&self) -> Vec<StreamingJobSummary> {
        let jobs_guard = self.active_jobs.read().unwrap();
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
        let jobs_guard = self.active_jobs.read().unwrap();
        let graph = jobs_guard.get(job_id)?;

        let uptime_secs = graph.start_time.elapsed().as_secs();
        let overall_status = Self::aggregate_pipeline_status(&graph.pipelines);

        let pipeline_details: Vec<PipelineDetail> = graph
            .pipelines
            .iter()
            .map(|(id, pipeline)| {
                let status = pipeline.status.read().unwrap().clone();
                PipelineDetail {
                    pipeline_id: *id,
                    status: format!("{status:?}"),
                }
            })
            .collect();

        let topology = render_program_topology(&graph.program);

        Some(StreamingJobDetail {
            job_id: graph.job_id.clone(),
            status: overall_status,
            pipeline_count: graph.pipelines.len() as i32,
            uptime_secs,
            pipelines: pipeline_details,
            topology,
        })
    }

    pub fn has_job(&self, job_id: &str) -> bool {
        self.active_jobs.read().unwrap().contains_key(job_id)
    }

    pub async fn remove_job(&self, job_id: &str, mode: StopMode) -> anyhow::Result<()> {
        {
            let jobs_guard = self.active_jobs.read().unwrap();
            if !jobs_guard.contains_key(job_id) {
                anyhow::bail!("Job not found: {job_id}");
            }
            let graph = &jobs_guard[job_id];
            let control_senders: Vec<_> =
                graph.pipelines.values().map(|p| p.control_tx.clone()).collect();

            drop(jobs_guard);

            for tx in control_senders {
                let _ = tx.send(ControlCommand::Stop { mode: mode.clone() }).await;
            }
        }

        self.active_jobs.write().unwrap().remove(job_id);
        info!(job_id = %job_id, "Job stopped and removed.");
        Ok(())
    }

    fn aggregate_pipeline_status(
        pipelines: &HashMap<u32, PhysicalPipeline>,
    ) -> String {
        let mut running = 0u32;
        let mut failed = 0u32;
        let mut finished = 0u32;
        let mut initializing = 0u32;

        for pipeline in pipelines.values() {
            match &*pipeline.status.read().unwrap() {
                PipelineStatus::Running => running += 1,
                PipelineStatus::Failed { .. } => failed += 1,
                PipelineStatus::Finished => finished += 1,
                PipelineStatus::Initializing => initializing += 1,
                PipelineStatus::Stopping => {}
            }
        }

        if failed > 0 {
            "DEGRADED".to_string()
        } else if running > 0 && running == pipelines.len() as u32 {
            "RUNNING".to_string()
        } else if finished == pipelines.len() as u32 {
            "FINISHED".to_string()
        } else if initializing > 0 {
            "INITIALIZING".to_string()
        } else {
            "PARTIAL".to_string()
        }
    }

    // ========================================================================

    fn build_operator_chain(
        &self,
        operator_configs: &[ChainedOperator],
    ) -> anyhow::Result<PreparedChain> {
        let mut source: Option<Box<dyn SourceOperator>> = None;
        let mut chain = Vec::with_capacity(operator_configs.len());

        for op_config in operator_configs {
            let constructed = self.operator_factory
                .create_operator(&op_config.operator_name, &op_config.operator_config)?;

            match constructed {
                ConstructedOperator::Operator(msg_op) => chain.push(msg_op),
                ConstructedOperator::Source(src_op) => {
                    if source.is_some() {
                        anyhow::bail!(
                            "Topology Error: Multiple source operators detected in one physical chain."
                        );
                    }
                    if !chain.is_empty() {
                        anyhow::bail!(
                            "Topology Error: Source operator '{}' cannot be scheduled inside a MessageOperator physical chain.",
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

    fn spawn_pipeline_thread(
        &self,
        job_id: String,
        pipeline_id: u32,
        operators: Vec<Box<dyn Operator>>,
        inboxes: Vec<BoxedEventStream>,
        outboxes: Vec<PhysicalSender>,
        control_rx: mpsc::Receiver<ControlCommand>,
        status: Arc<RwLock<PipelineStatus>>,
    ) -> anyhow::Result<std::thread::JoinHandle<()>> {
        let memory_pool = Arc::clone(&self.memory_pool);
        let thread_name = format!("Task-{job_id}-{pipeline_id}");

        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                *status.write().unwrap() = PipelineStatus::Running;

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build current-thread Tokio runtime for pipeline");

                let job_id_inner = job_id.clone();
                let execution_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(async move {
                        let ctx = TaskContext::new(
                            job_id_inner,
                            pipeline_id,
                            0,
                            1,
                            outboxes,
                            memory_pool,
                        );

                        let pipeline = Pipeline::new(operators, ctx, inboxes, control_rx)
                            .map_err(|e| anyhow::anyhow!("Pipeline init failed: {e}"))?;

                        pipeline.run().await.map_err(|e| anyhow::anyhow!("Pipeline execution failed: {e}"))
                    })
                }));

                Self::handle_pipeline_exit(&job_id, pipeline_id, execution_result, &status);
            })?;

        Ok(handle)
    }

    fn spawn_source_pipeline_thread(
        &self,
        job_id: String,
        pipeline_id: u32,
        source: Box<dyn SourceOperator>,
        operators: Vec<Box<dyn Operator>>,
        outboxes: Vec<PhysicalSender>,
        control_rx: mpsc::Receiver<ControlCommand>,
        status: Arc<RwLock<PipelineStatus>>,
    ) -> anyhow::Result<std::thread::JoinHandle<()>> {
        let memory_pool = Arc::clone(&self.memory_pool);
        let thread_name = format!("Task-{job_id}-{pipeline_id}");

        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                *status.write().unwrap() = PipelineStatus::Running;

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build current-thread Tokio runtime for source pipeline");

                let job_id_inner = job_id.clone();
                let execution_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(async move {
                        let ctx = TaskContext::new(
                            job_id_inner,
                            pipeline_id,
                            0,
                            1,
                            outboxes,
                            memory_pool,
                        );

                        let chain_head = ChainedDriver::build_chain(operators);
                        let runner = SourceRunner::new(source, chain_head, ctx, control_rx);

                        runner
                            .run()
                            .await
                            .map_err(|e| anyhow::anyhow!("Source pipeline execution failed: {e}"))
                    })
                }));

                Self::handle_pipeline_exit(&job_id, pipeline_id, execution_result, &status);
            })?;

        Ok(handle)
    }

    fn handle_pipeline_exit(
        job_id: &str,
        pipeline_id: u32,
        thread_result: std::thread::Result<anyhow::Result<()>>,
        status: &RwLock<PipelineStatus>,
    ) {
        let mut is_fatal = false;
        let final_status = match thread_result {
            Ok(Ok(_)) => {
                info!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline finished gracefully.");
                PipelineStatus::Finished
            }
            Ok(Err(e)) => {
                error!(job_id = %job_id, pipeline_id = pipeline_id, error = %e, "Pipeline failed.");
                is_fatal = true;
                PipelineStatus::Failed {
                    error: e.to_string(),
                    is_panic: false,
                }
            }
            Err(_) => {
                error!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline thread panicked!");
                is_fatal = true;
                PipelineStatus::Failed {
                    error: "Task thread encountered an unexpected panic".into(),
                    is_panic: true,
                }
            }
        };

        *status.write().unwrap() = final_status;

        if is_fatal {
            warn!(job_id = %job_id, pipeline_id = pipeline_id, "Pipeline failure detected, Job should be aborted or recovered.");
        }
    }
}
