use std::fmt;
use std::sync::Arc;

use crate::sql::common::FsSchema;
// ============ 强类型 ID (Strong-type IDs) ============

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubtaskIndex(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperatorUid(pub String);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SubtaskIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for OperatorUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============ 资源画像 (Resource Profile) ============

#[derive(Debug, Clone)]
pub struct ResourceProfile {
    pub managed_memory_bytes: u64,
    pub cpu_cores: f64,
    pub network_memory_bytes: u64,
}

impl Default for ResourceProfile {
    fn default() -> Self {
        Self {
            managed_memory_bytes: 64 * 1024 * 1024,
            cpu_cores: 1.0,
            network_memory_bytes: 32 * 1024 * 1024,
        }
    }
}

// ============ 分区策略 (Partitioning Strategy) ============

#[derive(Debug, Clone)]
pub enum PartitioningStrategy {
    Forward,
    HashByKeys(Vec<usize>),
    Rebalance,
}

// ============ 交换模式 (Exchange Mode) ============

#[derive(Debug, Clone)]
pub enum ExchangeMode {
    LocalThread,
    RemoteNetwork { target_addr: String },
}

// ============ 部署描述符 (Deployment Descriptors) ============

#[derive(Debug, Clone)]
pub struct TaskDeploymentDescriptor {
    pub job_id: JobId,
    pub vertex_id: VertexId,
    pub subtask_idx: SubtaskIndex,
    pub parallelism: u32,
    pub operator_name: String,
    pub operator_uid: OperatorUid,
    pub is_source: bool,
    pub operator_config_payload: Vec<u8>,
    pub resources: ResourceProfile,
    pub in_schemas: Vec<Arc<FsSchema>>,
    pub out_schema: Option<Arc<FsSchema>>,
    pub input_gates_count: usize,
    pub output_gates_count: usize,
}

#[derive(Debug, Clone)]
pub struct PhysicalEdgeDescriptor {
    pub src_vertex: VertexId,
    pub src_subtask: SubtaskIndex,
    pub dst_vertex: VertexId,
    pub dst_subtask: SubtaskIndex,
    pub partitioning: PartitioningStrategy,
    pub exchange_mode: ExchangeMode,
}

// ============ 执行图 (Execution Graph) ============

#[derive(Debug, Clone)]
pub struct ExecutionGraph {
    pub job_id: JobId,
    pub tasks: Vec<TaskDeploymentDescriptor>,
    pub edges: Vec<PhysicalEdgeDescriptor>,
}

impl ExecutionGraph {
    pub fn validate(&self) -> Result<(), String> {
        if self.tasks.is_empty() {
            return Err("Execution graph has no tasks".into());
        }
        if self.edges.is_empty() && self.tasks.len() > 1 {
            return Err("Multi-task graph has no edges".into());
        }
        let mut seen = std::collections::HashSet::new();
        for tdd in &self.tasks {
            if !seen.insert((tdd.vertex_id, tdd.subtask_idx)) {
                return Err(format!(
                    "Duplicate subtask: vertex={}, subtask={}",
                    tdd.vertex_id, tdd.subtask_idx
                ));
            }
        }
        Ok(())
    }
}
