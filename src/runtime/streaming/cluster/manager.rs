use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::ConstructedOperator;
use crate::runtime::streaming::cluster::graph::ExecutionGraph;
use crate::runtime::streaming::execution::runner::SubtaskRunner;
use crate::runtime::streaming::execution::source::SourceRunner;
use crate::runtime::streaming::factory::OperatorFactory;
use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::network::NetworkEnvironment;
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};
use crate::runtime::streaming::storage::manager::TableManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinSet;
use tracing::{error, info, instrument, warn};

pub struct TaskManager {
    pub worker_id: String,
    memory_pool: Arc<MemoryPool>,
    table_manager: Arc<tokio::sync::Mutex<TableManager>>,
    operator_factory: Arc<OperatorFactory>,
    task_supervisors: JoinSet<()>,
    pub controllers: HashMap<(u32, u32), Sender<ControlCommand>>,
}

impl TaskManager {
    pub fn new(
        worker_id: String,
        max_memory_bytes: usize,
        table_manager: Arc<tokio::sync::Mutex<TableManager>>,
        operator_factory: Arc<OperatorFactory>,
    ) -> Self {
        Self {
            worker_id,
            memory_pool: MemoryPool::new(max_memory_bytes),
            table_manager,
            operator_factory,
            task_supervisors: JoinSet::new(),
            controllers: HashMap::new(),
        }
    }

    #[instrument(skip(self, graph), fields(job_id = %graph.job_id))]
    pub async fn deploy_and_start(&mut self, graph: ExecutionGraph) -> anyhow::Result<()> {
        info!("TaskManager [{}] starting deployment...", self.worker_id);

        graph
            .validate()
            .map_err(|e| anyhow::anyhow!("Graph validation failed: {}", e))?;

        // 1. 网络连线期
        let local_queue_size = 1024;
        let mut network_env = NetworkEnvironment::build_from_graph(&graph, local_queue_size);

        // 2. 控制通道初始化
        let mut control_rxs = HashMap::new();
        for tdd in &graph.tasks {
            let key = (tdd.vertex_id.0, tdd.subtask_idx.0);
            let (ctrl_tx, ctrl_rx) = channel(32);
            self.controllers.insert(key, ctrl_tx);
            control_rxs.insert(key, ctrl_rx);
        }

        // 3. 部署与算子实例化
        for tdd in graph.tasks {
            let v_id = tdd.vertex_id;
            let s_idx = tdd.subtask_idx;
            let key = (v_id.0, s_idx.0);

            let ctrl_rx = control_rxs.remove(&key).unwrap();
            let inboxes = network_env.take_inboxes(v_id, s_idx);
            let outboxes = network_env.take_outboxes(v_id, s_idx);

            let ctx = TaskContext::new(
                tdd.job_id.0.clone(),
                v_id.0,
                s_idx.0,
                tdd.parallelism,
                outboxes,
                self.memory_pool.clone(),
                Some(self.table_manager.clone()),
            );

            let constructed_op = self.operator_factory.create_operator(
                &tdd.operator_name,
                &tdd.operator_config_payload,
            )?;

            // 4. 任务发射入监督树
            let worker_id = self.worker_id.clone();
            match constructed_op {
                ConstructedOperator::Source(source_op) => {
                    let runner = SourceRunner::new(source_op, ctx, ctrl_rx);
                    self.task_supervisors.spawn(async move {
                        if let Err(e) = runner.run().await {
                            error!(
                                worker = %worker_id,
                                vertex = key.0,
                                subtask = key.1,
                                "SourceTask CRASHED: {:?}", e
                            );
                            panic!("SourceTask failed");
                        }
                    });
                }
                ConstructedOperator::Operator(msg_op) => {
                    let runner = SubtaskRunner::new(msg_op, ctx, inboxes, ctrl_rx);
                    self.task_supervisors.spawn(async move {
                        if let Err(e) = runner.run().await {
                            error!(
                                worker = %worker_id,
                                vertex = key.0,
                                subtask = key.1,
                                "StreamTask CRASHED: {:?}", e
                            );
                            panic!("StreamTask failed");
                        }
                    });
                }
            }
        }

        info!(
            "TaskManager [{}] deployment complete. All tasks ignited.",
            self.worker_id
        );
        Ok(())
    }

    /// 监控运行状态：Supervisor 模式防止级联崩溃
    pub async fn wait_and_supervise(mut self) {
        while let Some(result) = self.task_supervisors.join_next().await {
            match result {
                Ok(_) => {
                    info!("A subtask finished successfully.");
                }
                Err(join_error) => {
                    if join_error.is_panic() {
                        error!(
                            "FATAL: A subtask panicked! Initiating emergency shutdown \
                             of the entire TaskManager to prevent data corruption."
                        );
                        self.task_supervisors.abort_all();
                        break;
                    } else if join_error.is_cancelled() {
                        warn!("A subtask was cancelled.");
                    }
                }
            }
        }
        info!("TaskManager shutdown process complete.");
    }

    pub async fn stop_all(&self, mode: StopMode) {
        for (key, tx) in &self.controllers {
            if let Err(e) = tx
                .send(ControlCommand::Stop { mode: mode.clone() })
                .await
            {
                warn!("Failed to send stop command to task {:?}: {}", key, e);
            }
        }
    }
}
