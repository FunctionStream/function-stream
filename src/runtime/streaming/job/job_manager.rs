use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use protocol::grpc::api::{ChainedOperator, FsProgram};
use tokio::sync::mpsc;
use tracing::error;

use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::factory::OperatorFactory;
use crate::runtime::streaming::job::edge_manager::EdgeManager;
use crate::runtime::streaming::job::models::{PhysicalExecutionGraph, PhysicalPipeline, PipelineStatus};
use crate::runtime::streaming::job::pipeline_runner::{FusionOperatorChain, PipelineRunner};
use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};
use crate::runtime::streaming::storage::manager::TableManager;

pub struct JobManager {
    active_jobs: Arc<RwLock<HashMap<String, PhysicalExecutionGraph>>>,
    operator_factory: Arc<OperatorFactory>,
    memory_pool: Arc<MemoryPool>,
    table_manager: Option<Arc<tokio::sync::Mutex<TableManager>>>,
}

impl JobManager {
    pub fn new(
        operator_factory: Arc<OperatorFactory>,
        max_memory_bytes: usize,
        table_manager: Option<Arc<tokio::sync::Mutex<TableManager>>>,
    ) -> Self {
        Self {
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            operator_factory,
            memory_pool: MemoryPool::new(max_memory_bytes),
            table_manager,
        }
    }

    /// 从逻辑计划点火物理线程
    pub async fn submit_job(&self, program: FsProgram) -> anyhow::Result<String> {
        let job_id = format!("job-{}", chrono::Utc::now().timestamp_millis());

        let mut edge_manager = EdgeManager::build(&program.nodes, &program.edges);
        let mut physical_pipelines = HashMap::new();

        for node in &program.nodes {
            let pipe_id = node.node_index as u32;
            let (inbox, outboxes) = edge_manager.take_endpoints(pipe_id);
            let chain = self.create_chain(&node.operators)?;
            let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
            let status = Arc::new(RwLock::new(PipelineStatus::Initializing));

            let thread_status = status.clone();
            let job_id_for_thread = job_id.clone();
            let exit_job_id = job_id_for_thread.clone();
            let registry_ptr = self.active_jobs.clone();
            let memory_pool = self.memory_pool.clone();
            let table_manager = self.table_manager.clone();

            let handle = std::thread::Builder::new()
                .name(format!("Job-{}-Pipe-{}", job_id, pipe_id))
                .spawn(move || {
                    {
                        let mut st = thread_status.write().unwrap();
                        *st = PipelineStatus::Running;
                    }

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build current thread runtime");

                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        rt.block_on(async move {
                            let mut runner = PipelineRunner::new(
                                pipe_id,
                                chain,
                                inbox,
                                outboxes,
                                ctrl_rx,
                                job_id_for_thread.clone(),
                                memory_pool,
                                table_manager,
                            );
                            runner.run().await
                        })
                    }));

                    Self::on_pipeline_exit(exit_job_id, pipe_id, result, thread_status, registry_ptr);
                })?;

            physical_pipelines.insert(
                pipe_id,
                PhysicalPipeline {
                    pipeline_id: pipe_id,
                    handle: Some(handle),
                    status,
                    control_tx: ctrl_tx,
                },
            );
        }

        let graph = PhysicalExecutionGraph {
            job_id: job_id.clone(),
            program,
            pipelines: physical_pipelines,
            start_time: std::time::Instant::now(),
        };

        self.active_jobs.write().unwrap().insert(job_id.clone(), graph);
        Ok(job_id)
    }

    pub async fn stop_job(&self, job_id: &str, mode: StopMode) -> anyhow::Result<()> {
        let controllers = {
            let jobs = self.active_jobs.read().unwrap();
            let graph = jobs
                .get(job_id)
                .ok_or_else(|| anyhow::anyhow!("job not found: {job_id}"))?;
            graph
                .pipelines
                .values()
                .map(|p| p.control_tx.clone())
                .collect::<Vec<_>>()
        };

        for tx in controllers {
            tx.send(ControlCommand::Stop { mode: mode.clone() }).await?;
        }
        Ok(())
    }

    pub fn get_pipeline_statuses(&self, job_id: &str) -> Option<HashMap<u32, PipelineStatus>> {
        let jobs = self.active_jobs.read().unwrap();
        let graph = jobs.get(job_id)?;
        Some(
            graph
                .pipelines
                .iter()
                .map(|(id, pipeline)| (*id, pipeline.status.read().unwrap().clone()))
                .collect(),
        )
    }

    fn create_chain(&self, operators: &[ChainedOperator]) -> anyhow::Result<FusionOperatorChain> {
        let mut chain = Vec::with_capacity(operators.len());
        for op in operators {
            match self
                .operator_factory
                .create_operator(&op.operator_name, &op.operator_config)?
            {
                ConstructedOperator::Operator(msg_op) => chain.push(msg_op),
                ConstructedOperator::Source(_) => {
                    return Err(anyhow::anyhow!(
                        "source operator '{}' cannot be used inside a physical pipeline chain",
                        op.operator_name
                    ));
                }
            }
        }
        Ok(FusionOperatorChain::new(chain))
    }

    fn on_pipeline_exit(
        job_id: String,
        pipe_id: u32,
        result: std::thread::Result<anyhow::Result<()>>,
        status: Arc<RwLock<PipelineStatus>>,
        _registry: Arc<RwLock<HashMap<String, PhysicalExecutionGraph>>>,
    ) {
        let mut needs_abort = false;
        match result {
            Ok(Err(e)) => {
                *status.write().unwrap() = PipelineStatus::Failed {
                    error: e.to_string(),
                    is_panic: false,
                };
                needs_abort = true;
            }
            Err(_) => {
                *status.write().unwrap() = PipelineStatus::Failed {
                    error: "panic".into(),
                    is_panic: true,
                };
                needs_abort = true;
            }
            Ok(Ok(_)) => {
                *status.write().unwrap() = PipelineStatus::Finished;
            }
        }

        if needs_abort {
            error!(
                "Pipeline {}-{} failed. Initiating Job Abort.",
                job_id, pipe_id
            );
        }
    }
}
