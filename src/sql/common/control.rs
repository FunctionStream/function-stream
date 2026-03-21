use std::collections::HashMap;
use std::time::SystemTime;

use super::message::CheckpointBarrier;

/// Control messages sent from the controller to worker tasks.
#[derive(Debug, Clone)]
pub enum ControlMessage {
    Checkpoint(CheckpointBarrier),
    Stop {
        mode: StopMode,
    },
    Commit {
        epoch: u32,
        commit_data: HashMap<String, HashMap<u32, Vec<u8>>>,
    },
    LoadCompacted {
        compacted: CompactionResult,
    },
    NoOp,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StopMode {
    Graceful,
    Immediate,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub operator_id: String,
    pub compacted_tables: HashMap<String, TableCheckpointMetadata>,
}

#[derive(Debug, Clone)]
pub struct TableCheckpointMetadata {
    pub table_type: TableType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    GlobalKeyValue,
    ExpiringKeyedTimeTable,
}

/// Responses sent from worker tasks back to the controller.
#[derive(Debug, Clone)]
pub enum ControlResp {
    CheckpointEvent(CheckpointEvent),
    CheckpointCompleted(CheckpointCompleted),
    TaskStarted {
        node_id: u32,
        task_index: usize,
        start_time: SystemTime,
    },
    TaskFinished {
        node_id: u32,
        task_index: usize,
    },
    TaskFailed {
        node_id: u32,
        task_index: usize,
        error: TaskError,
    },
    Error {
        node_id: u32,
        operator_id: String,
        task_index: usize,
        message: String,
        details: String,
    },
}

#[derive(Debug, Clone)]
pub struct CheckpointCompleted {
    pub checkpoint_epoch: u32,
    pub node_id: u32,
    pub operator_id: String,
    pub subtask_metadata: SubtaskCheckpointMetadata,
}

#[derive(Debug, Clone)]
pub struct SubtaskCheckpointMetadata {
    pub subtask_index: u32,
    pub start_time: u64,
    pub finish_time: u64,
    pub watermark: Option<u64>,
    pub bytes: u64,
    pub table_metadata: HashMap<String, TableSubtaskCheckpointMetadata>,
    pub table_configs: HashMap<String, TableConfig>,
}

#[derive(Debug, Clone)]
pub struct TableSubtaskCheckpointMetadata {
    pub subtask_index: u32,
    pub table_type: TableType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub table_type: TableType,
    pub config: Vec<u8>,
    pub state_version: u32,
}

#[derive(Debug, Clone)]
pub struct CheckpointEvent {
    pub checkpoint_epoch: u32,
    pub node_id: u32,
    pub operator_id: String,
    pub subtask_index: u32,
    pub time: SystemTime,
    pub event_type: TaskCheckpointEventType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCheckpointEventType {
    StartedAlignment,
    StartedCheckpointing,
    FinishedOperatorSetup,
    FinishedSync,
    FinishedCommit,
}

#[derive(Debug, Clone)]
pub struct TaskError {
    pub job_id: String,
    pub node_id: u32,
    pub operator_id: String,
    pub operator_subtask: u64,
    pub error: String,
    pub error_domain: ErrorDomain,
    pub retry_hint: RetryHint,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorDomain {
    User,
    Internal,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryHint {
    NoRetry,
    WithBackoff,
}
