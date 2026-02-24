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

use crate::runtime::common::TaskCompletionFlag;
use crate::runtime::processor::function_error::{ErrorReporter, FunctionErrorReport};
use crossbeam_channel::Sender;
use std::sync::Arc;

#[derive(Clone)]
pub enum TaskControlSignal {
    Start {
        completion_flag: TaskCompletionFlag,
    },
    Stop {
        completion_flag: TaskCompletionFlag,
    },
    Cancel {
        completion_flag: TaskCompletionFlag,
    },
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    Close {
        completion_flag: TaskCompletionFlag,
    },
    ErrorReport(FunctionErrorReport),
}

pub struct ControlMailBox {
    sender: Sender<TaskControlSignal>,
}

impl ControlMailBox {
    pub fn new(sender: Sender<TaskControlSignal>) -> Self {
        Self { sender }
    }

    fn send_signal(
        &self,
        signal: TaskControlSignal,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.sender.send(signal).map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Failed to send {} signal: {}",
                operation_name, e
            ))) as Box<dyn std::error::Error + Send>
        })
    }

    pub fn send_start(
        &self,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.send_signal(
            TaskControlSignal::Start {
                completion_flag: completion_flag.clone(),
            },
            "start",
        )
    }

    pub fn send_stop(
        &self,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.send_signal(
            TaskControlSignal::Stop {
                completion_flag: completion_flag.clone(),
            },
            "stop",
        )
    }

    pub fn send_cancel(
        &self,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.send_signal(
            TaskControlSignal::Cancel {
                completion_flag: completion_flag.clone(),
            },
            "cancel",
        )
    }

    pub fn send_checkpoint(
        &self,
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.send_signal(
            TaskControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            },
            "checkpoint",
        )
    }

    pub fn send_finish_checkpoint(
        &self,
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.send_signal(
            TaskControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            },
            "checkpoint finish",
        )
    }

    pub fn send_close(
        &self,
        completion_flag: TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.sender
            .send(TaskControlSignal::Close {
                completion_flag: completion_flag.clone(),
            })
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to send close signal: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })
    }

    pub fn send_error_report(
        &self,
        report: FunctionErrorReport,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        self.sender
            .send(TaskControlSignal::ErrorReport(report))
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to send error report: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })
    }
}
