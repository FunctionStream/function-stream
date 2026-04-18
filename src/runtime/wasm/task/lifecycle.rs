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

// Task Lifecycle - Task lifecycle management interface
//
// Defines the complete lifecycle management interface for Task, including initialization, start, stop, checkpoint and close

use crate::runtime::common::ComponentState;
use crate::runtime::wasm::task::control_mailbox::ControlMailBox;
use crate::runtime::wasm::taskexecutor::InitContext;
use crate::storage::task::FunctionInfo;
use std::sync::Arc;

/// Task lifecycle management interface
///
/// Defines complete lifecycle management methods for Task, following standard state transition flow:
/// ```ignore
/// Uninitialized -> Initialized -> Starting -> Running
///                                             |
///                                             v
///                                          Checkpointing
///                                             |
///                                             v
///                                          Stopping -> Stopped
///                                             |
///                                             v
///                                          Closing -> Closed
/// ```
///
/// All methods should be called in appropriate thread context and follow state machine transition rules.
pub trait TaskLifecycle: Send + Sync {
    /// Initialize task with initialization context
    ///
    /// Called before task is used to perform necessary initialization work, including:
    /// - Load configuration
    /// - Initialize resources
    /// - Prepare runtime environment
    ///
    /// State transition: Uninitialized -> Initialized
    ///
    /// # Arguments
    /// - `init_context`: Initialization context containing state storage, task storage and other resources
    ///
    /// # Returns
    /// - `Ok(())`: Initialization successful
    /// - `Err(...)`: Initialization failed
    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Start task
    ///
    /// Start task execution, begin processing data stream.
    /// Before calling this method, the task should have completed initialization.
    ///
    /// State transition: Initialized/Stopped -> Starting -> Running
    ///
    /// # Returns
    /// - `Ok(())`: Start successful
    /// - `Err(...)`: Start failed
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Stop task
    ///
    /// Stop task execution, but keep resources available, can be restarted.
    /// After stopping, the task no longer processes new data, but processed data state is preserved.
    ///
    /// State transition: Running/Checkpointing -> Stopping -> Stopped
    ///
    /// # Returns
    /// - `Ok(())`: Stop successful
    /// - `Err(...)`: Stop failed
    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Execute checkpoint
    ///
    /// Save current task state for failure recovery.
    /// Checkpoint operation should be atomic, ensuring state consistency.
    ///
    /// State transition: Running -> Checkpointing -> Running
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID for identification and recovery
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint successful
    /// - `Err(...)`: Checkpoint failed
    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Close task
    ///
    /// Release all task resources, the task will no longer be usable.
    /// Before closing, task execution should be stopped first.
    ///
    /// State transition: Running/Stopped -> Closing -> Closed
    ///
    /// # Returns
    /// - `Ok(())`: Close successful
    /// - `Err(...)`: Close failed
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Get current state
    ///
    /// Returns the current lifecycle state of the task.
    ///
    /// # Returns
    /// Current state of the task
    fn get_state(&self) -> ComponentState;

    /// Get task name
    ///
    /// Returns the name of the task.
    ///
    /// # Returns
    /// Name of the task
    fn get_name(&self) -> &str;

    fn get_function_info(&self) -> FunctionInfo;

    fn get_control_mailbox(&self) -> Option<Arc<ControlMailBox>> {
        None
    }

    /// Check if task is running
    ///
    /// # Returns
    /// - `true`: Task is running (Running or Checkpointing state)
    /// - `false`: Task is not running
    fn is_running(&self) -> bool {
        self.get_state().is_running()
    }

    /// Check if task is closed
    ///
    /// # Returns
    /// - `true`: Task is closed
    /// - `false`: Task is not closed
    fn is_closed(&self) -> bool {
        self.get_state().is_closed()
    }

    /// Check if task is in error state
    ///
    /// # Returns
    /// - `true`: Task is in error state
    /// - `false`: Task is not in error state
    fn is_error(&self) -> bool {
        self.get_state().is_error()
    }
}
