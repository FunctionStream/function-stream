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

// OutputSink - Output sink interface
//
// Output sink interface supporting lifecycle management and data sending

use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::taskexecutor::InitContext;

/// OutputSink - Output sink interface
///
/// Supports complete lifecycle management and data output functionality
pub trait OutputSink: Send + Sync {
    /// Initialize output sink with initialization context
    ///
    /// Called before use to perform necessary initialization work
    ///
    /// # Arguments
    /// - `init_context`: Initialization context containing state storage, task storage and other resources
    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Start output sink
    ///
    /// Start sending data to external systems
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Stop output sink
    ///
    /// Stop sending data, but keep resources available
    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Close output sink
    ///
    /// Release all resources, the sink will no longer be usable
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Collect data
    ///
    /// Collect BufferOrEvent into output sink
    ///
    /// # Arguments
    /// - `data`: Data to collect
    ///
    /// # Returns
    /// - `Ok(())`: Collection successful
    /// - `Err(...)`: Collection failed
    fn collect(&mut self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Restore state
    ///
    /// Restore output sink state from checkpoint
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID
    ///
    /// # Returns
    /// - `Ok(())`: Restore successful
    /// - `Err(...)`: Restore failed
    fn restore_state(
        &mut self,
        _checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: state restoration not supported
        Ok(())
    }

    /// Start checkpoint
    ///
    /// Start saving current output sink state for failure recovery
    /// State transition: Running -> Checkpointing
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint start successful
    /// - `Err(...)`: Checkpoint start failed
    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Finish checkpoint
    ///
    /// Notify output sink that checkpoint is complete
    /// State transition: Checkpointing -> Running
    ///
    /// # Arguments
    /// - `checkpoint_id`: Checkpoint ID
    ///
    /// # Returns
    /// - `Ok(())`: Checkpoint finish successful
    /// - `Err(...)`: Checkpoint finish failed
    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Flush buffered data
    ///
    /// Ensure all buffered data is sent out
    ///
    /// # Returns
    /// - `Ok(())`: Flush successful
    /// - `Err(...)`: Flush failed
    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(()) // Default implementation: no flush needed
    }

    /// Clone OutputSink (returns Box<dyn OutputSink>)
    ///
    /// Used to create a clone of OutputSink
    ///
    /// # Returns
    /// - `Box<dyn OutputSink>`: Cloned OutputSink instance
    fn box_clone(&self) -> Box<dyn OutputSink>;
}
