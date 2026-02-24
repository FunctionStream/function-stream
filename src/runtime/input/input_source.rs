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

// InputSource - Input source interface
//
// Defines the standard interface for input sources, including lifecycle management and data retrieval
// State is uniformly managed by the runloop thread

use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::taskexecutor::InitContext;

// Re-export common component state for compatibility
pub use crate::runtime::common::ComponentState as InputSourceState;

/// InputSource - Input source interface
///
/// Defines the standard interface for input sources, including:
/// - Lifecycle management (init, start, stop, close)
/// - Data retrieval (get_next, poll_next)
/// - Checkpoint support (take_checkpoint, finish_checkpoint)
///
/// State is uniformly managed by the runloop thread, callers don't need to directly manipulate state
pub trait InputSource: Send + Sync {
    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Start input source
    ///
    /// Start reading data from input source
    /// State is set to Running by the runloop thread
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Stop input source
    ///
    /// Stop reading data from input source, but keep resources available
    /// State is set to Stopped by the runloop thread
    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Close input source
    ///
    /// Release all resources, the input source will no longer be usable
    /// State is set to Closed by the runloop thread
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Get next data
    ///
    /// Get next BufferOrEvent from input source
    /// Returns None to indicate no data is currently available (non-blocking)
    ///
    /// # Returns
    /// - `Ok(Some(BufferOrEvent))`: Data retrieved
    /// - `Ok(None)`: No data currently available
    /// - `Err(...)`: Error occurred
    fn get_next(&mut self) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>>;

    /// Poll for next data (non-blocking)
    ///
    /// Poll for next BufferOrEvent from input source without blocking current thread
    /// If no data is currently available, returns None immediately
    ///
    /// # Returns
    /// - `Ok(Some(BufferOrEvent))`: Data retrieved
    /// - `Ok(None)`: No data currently available (non-blocking return)
    /// - `Err(...)`: Error occurred
    fn poll_next(&mut self) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        self.get_next()
    }

    /// Start checkpoint
    ///
    /// Start saving current input source state for failure recovery
    /// State is set to Checkpointing by the runloop thread
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID
    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Finish checkpoint
    ///
    /// Notify input source that checkpoint is complete
    /// State is set back to Running by the runloop thread
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID
    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn get_group_id(&self) -> usize;

    fn set_error_state(&self) -> Result<(), Box<dyn std::error::Error + Send>>;
}
