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

// Component State - Task component state machine
//
// Defines common state and control mechanisms for all task components (Input, Output, Processor, etc.)
//
// This is a pure state machine definition without any interface constraints
// Each component can choose how to use these states according to its own needs

/// Control task channel capacity (maximum number of tasks in fixed-length channel)
/// Since control tasks (CheckPoint, Stop, Close) have low frequency, capacity doesn't need to be too large
pub const CONTROL_TASK_CHANNEL_CAPACITY: usize = 10;

/// Task component state
///
/// Represents the lifecycle state of task components (Input, Output, Processor, etc.)
///
/// State transition diagram:
/// ```ignore
/// Uninitialized -> Initialized -> Starting -> Running
///                                             |
///                                             v
///                                          Checkpointing (checkpointing)
///                                             |
///                                             v
///                                          Stopping -> Stopped
///                                             |
///                                             v
///                                          Closing -> Closed
///                                             
///                                          Error (any state can transition to error)
/// ```
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ComponentState {
    /// Uninitialized
    #[default]
    Uninitialized,
    /// Initialized
    Initialized,
    /// Starting
    Starting,
    /// Running
    Running,
    /// Checkpointing
    Checkpointing,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
    /// Closing
    Closing,
    /// Closed
    Closed,
    /// Error state
    Error {
        /// Error message
        error: String,
    },
}

impl ComponentState {
    /// Check if state can accept new operations
    pub fn can_accept_operations(&self) -> bool {
        matches!(
            self,
            ComponentState::Initialized | ComponentState::Running | ComponentState::Stopped
        )
    }

    /// Check if state is running
    pub fn is_running(&self) -> bool {
        matches!(
            self,
            ComponentState::Running | ComponentState::Checkpointing
        )
    }

    /// Check if state is closed
    pub fn is_closed(&self) -> bool {
        matches!(self, ComponentState::Closed)
    }

    /// Check if state is in error state
    pub fn is_error(&self) -> bool {
        matches!(self, ComponentState::Error { .. })
    }

    /// Check if can transition from current state to target state
    pub fn can_transition_to(&self, target: &ComponentState) -> bool {
        use ComponentState::*;

        match (self, target) {
            // Can transition from Uninitialized to Initialized
            (Uninitialized, Initialized) => true,

            // Can transition from Initialized to Starting
            (Initialized, Starting) => true,

            // Can transition from Starting to Running
            (Starting, Running) => true,

            // Can transition from Running to Checkpointing
            (Running, Checkpointing) => true,

            // Can transition from Checkpointing back to Running
            (Checkpointing, Running) => true,

            // Can transition from Running or Checkpointing to Stopping
            (Running, Stopping) | (Checkpointing, Stopping) => true,

            // Can transition from Stopping to Stopped
            (Stopping, Stopped) => true,

            // Can restart from Stopped
            (Stopped, Starting) => true,

            // Can transition from Running, Checkpointing, or Stopped to Closing
            (Running, Closing) | (Checkpointing, Closing) | (Stopped, Closing) => true,

            // Can transition from Closing to Closed
            (Closing, Closed) => true,

            // Any state can transition to Error state
            (_, Error { .. }) => true,

            // Other transitions are not allowed
            _ => false,
        }
    }
}

impl std::fmt::Display for ComponentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentState::Uninitialized => write!(f, "Uninitialized"),
            ComponentState::Initialized => write!(f, "Initialized"),
            ComponentState::Starting => write!(f, "Starting"),
            ComponentState::Running => write!(f, "Running"),
            ComponentState::Checkpointing => write!(f, "Checkpointing"),
            ComponentState::Stopping => write!(f, "Stopping"),
            ComponentState::Stopped => write!(f, "Stopped"),
            ComponentState::Closing => write!(f, "Closing"),
            ComponentState::Closed => write!(f, "Closed"),
            ComponentState::Error { error } => write!(f, "Error({})", error),
        }
    }
}

/// Control task type
///
/// Used to pass various control tasks between component threads and main thread
/// All task components should support these control tasks
#[derive(Debug, Clone)]
pub enum ControlTask {
    /// Checkpoint task
    Checkpoint {
        /// Checkpoint ID
        checkpoint_id: u64,
        /// Timestamp (optional)
        timestamp: Option<u64>,
    },
    /// Stop task
    Stop {
        /// Stop reason (optional)
        reason: Option<String>,
    },
    /// Close task
    Close {
        /// Close reason (optional)
        reason: Option<String>,
    },
    /// Error task
    Error {
        /// Error message
        error: String,
        /// Error type (optional)
        error_type: Option<String>,
        /// Whether component should be stopped
        should_stop: bool,
    },
}
