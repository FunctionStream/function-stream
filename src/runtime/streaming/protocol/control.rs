//! 控制平面：与 [`super::event::StreamEvent`] 队列分离的高优先级指令。

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use crate::sql::common::CheckpointBarrier;

/// 可序列化的 barrier 载荷（`CheckpointBarrier` 本身未实现 `serde`，供 RPC / 持久化使用）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointBarrierWire {
    pub epoch: u32,
    pub min_epoch: u32,
    pub timestamp_secs: u64,
    pub timestamp_subsec_nanos: u32,
    pub then_stop: bool,
}

impl From<CheckpointBarrier> for CheckpointBarrierWire {
    fn from(b: CheckpointBarrier) -> Self {
        let d = b
            .timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            epoch: b.epoch,
            min_epoch: b.min_epoch,
            timestamp_secs: d.as_secs(),
            timestamp_subsec_nanos: d.subsec_nanos(),
            then_stop: b.then_stop,
        }
    }
}

impl From<CheckpointBarrierWire> for CheckpointBarrier {
    fn from(w: CheckpointBarrierWire) -> Self {
        Self {
            epoch: w.epoch,
            min_epoch: w.min_epoch,
            timestamp: std::time::UNIX_EPOCH
                + Duration::new(w.timestamp_secs, w.timestamp_subsec_nanos),
            then_stop: w.then_stop,
        }
    }
}

/// JobManager / 调度器下发的高优控制指令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    Start,
    Stop { mode: StopMode },
    DropState,
    Commit { epoch: u32 },
    UpdateConfig { config_json: String },
    /// 通常由 [`crate::runtime::streaming::SourceRunner`] 接收，源头落盘后向下游注入 `Barrier`。
    TriggerCheckpoint { barrier: CheckpointBarrierWire },
}

impl ControlCommand {
    pub fn trigger_checkpoint(barrier: CheckpointBarrier) -> Self {
        Self::TriggerCheckpoint {
            barrier: barrier.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopMode {
    Graceful,
    Immediate,
}

pub fn control_channel(capacity: usize) -> (Sender<ControlCommand>, Receiver<ControlCommand>) {
    mpsc::channel(capacity)
}
