use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use protocol::grpc::api::FsProgram;
use tokio::sync::mpsc;

use crate::runtime::streaming::protocol::control::ControlCommand;

/// 物理 Pipeline 的实时状态
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStatus {
    Initializing,
    Running,
    Failed { error: String, is_panic: bool },
    Finished,
    Stopping,
}

/// 物理执行图中的一个执行单元
pub struct PhysicalPipeline {
    pub pipeline_id: u32,
    pub handle: Option<JoinHandle<()>>,
    pub status: Arc<RwLock<PipelineStatus>>,
    pub control_tx: mpsc::Sender<ControlCommand>,
}

/// 一个 SQL Job 的物理执行图
pub struct PhysicalExecutionGraph {
    pub job_id: String,
    pub program: FsProgram,
    pub pipelines: HashMap<u32, PhysicalPipeline>,
    pub start_time: Instant,
}
