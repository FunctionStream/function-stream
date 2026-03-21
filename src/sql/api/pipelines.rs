use super::udfs::Udf;
use crate::sql::common::control::ErrorDomain;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ValidateQueryPost {
    pub query: String,
    pub udfs: Option<Vec<Udf>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct QueryValidationResult {
    pub graph: Option<PipelineGraph>,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelinePost {
    pub name: String,
    pub query: String,
    pub udfs: Option<Vec<Udf>>,
    pub parallelism: u64,
    pub checkpoint_interval_micros: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PreviewPost {
    pub query: String,
    pub udfs: Option<Vec<Udf>>,
    #[serde(default)]
    pub enable_sinks: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelinePatch {
    pub parallelism: Option<u64>,
    pub checkpoint_interval_micros: Option<u64>,
    pub stop: Option<StopType>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelineRestart {
    pub force: Option<bool>,
    pub ignore_state: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
    pub checkpoint_interval_micros: u64,
    pub stop: StopType,
    pub created_at: u64,
    pub action: Option<StopType>,
    pub action_text: String,
    pub action_in_progress: bool,
    pub graph: PipelineGraph,
    pub preview: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelineGraph {
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelineNode {
    pub node_id: u32,
    pub operator: String,
    pub description: String,
    pub parallelism: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PipelineEdge {
    pub src_id: u32,
    pub dest_id: u32,
    pub key_type: String,
    pub value_type: String,
    pub edge_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StopType {
    None,
    Checkpoint,
    Graceful,
    Immediate,
    Force,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct FailureReason {
    pub error: String,
    pub domain: ErrorDomain,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Job {
    pub id: String,
    pub running_desired: bool,
    pub state: String,
    pub run_id: u64,
    pub start_time: Option<u64>,
    pub finish_time: Option<u64>,
    pub tasks: Option<u64>,
    pub failure_reason: Option<FailureReason>,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JobLogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct JobLogMessage {
    pub id: String,
    pub created_at: u64,
    pub operator_id: Option<String>,
    pub task_index: Option<u64>,
    pub level: JobLogLevel,
    pub message: String,
    pub details: String,
    pub error_domain: Option<ErrorDomain>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct OutputData {
    pub operator_id: String,
    pub subtask_idx: u32,
    pub timestamps: Vec<u64>,
    pub start_id: u64,
    pub batch: String,
}
