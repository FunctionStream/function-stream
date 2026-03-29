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


use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use crate::sql::common::CheckpointBarrier;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    Start,
    Stop { mode: StopMode },
    DropState,
    Commit { epoch: u32 },
    UpdateConfig { config_json: String },
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
