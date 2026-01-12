// WasmProcessor trait definition
//
// This module defines the WasmProcessor trait, which is the interface
// for WebAssembly-based data processors in the stream processing system.

use crate::runtime::taskexecutor::InitContext;
use std::any::Any;

/// WASM Processor trait
///
/// This trait defines the interface for processing data using WebAssembly modules.
/// Implementations should load and execute WASM modules to process stream data.
pub trait WasmProcessor: Send + Sync {
    /// Process input data
    ///
    /// # Arguments
    /// * `data` - Input data as bytes
    /// * `input_index` - Index of the input source (0-based)
    ///
    /// # Note
    /// The actual processed data is sent via collector::emit in WASM
    fn process(
        &self,
        data: Vec<u8>,
        input_index: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Process watermark
    ///
    /// # Arguments
    /// * `timestamp` - Watermark timestamp
    /// * `input_index` - Index of the input source that generated the watermark (0-based)
    ///
    /// # Returns
    /// Ok(()) if processing succeeds, or an error if it fails
    fn process_watermark(
        &mut self,
        timestamp: u64,
        input_index: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!(
            "Processing watermark: {} from input {}",
            timestamp,
            input_index
        );
        Ok(())
    }

    /// Initialize processor with initialization context
    ///
    /// This method should:
    /// 1. Load the WASM module from the file system
    /// 2. Validate the module
    /// 3. Prepare the module for execution
    ///
    /// # Arguments
    /// - `init_context`: Initialization context containing state storage, task storage and other resources
    ///
    /// # Returns
    /// Ok(()) if initialization succeeds, or an error if it fails
    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Take a checkpoint
    ///
    /// This method should:
    /// 1. Save the current state of the WASM module
    /// 2. Save any internal state (watermark, buffers, etc.)
    /// 3. Persist the checkpoint to storage
    ///
    /// # Arguments
    /// * `checkpoint_id` - Unique identifier for this checkpoint
    ///
    /// # Returns
    /// Ok(()) if checkpoint succeeds, or an error if it fails
    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!("Taking checkpoint: {}", checkpoint_id);
        Ok(())
    }

    /// Finish a checkpoint
    ///
    /// This method should:
    /// 1. Finalize the checkpoint
    /// 2. Commit checkpoint data to storage
    /// 3. Clean up temporary checkpoint files
    ///
    /// # Arguments
    /// * `checkpoint_id` - Unique identifier for this checkpoint
    ///
    /// # Returns
    /// Ok(()) if checkpoint finish succeeds, or an error if it fails
    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!("Finishing checkpoint: {}", checkpoint_id);
        Ok(())
    }

    /// Restore state from checkpoint
    ///
    /// This method should:
    /// 1. Load checkpoint data from storage
    /// 2. Restore WASM module state
    /// 3. Restore internal state (watermark, buffers, etc.)
    /// 4. Reinitialize the processor with restored state
    ///
    /// # Arguments
    /// * `checkpoint_id` - Unique identifier for the checkpoint to restore from
    ///
    /// # Returns
    /// Ok(()) if restore succeeds, or an error if it fails
    fn restore_state(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!("Restoring state from checkpoint: {}", checkpoint_id);
        Ok(())
    }

    /// Check if the processor is healthy
    ///
    /// This method should check the health status of the processor,
    /// including whether it's initialized, if there are any errors,
    /// and if the WASM module is functioning correctly.
    ///
    /// # Returns
    /// `true` if the processor is healthy, `false` otherwise
    fn is_healthy(&self) -> bool {
        // Default implementation: always healthy
        true
    }

    /// Close the processor and clean up resources
    ///
    /// This method should:
    /// 1. Clean up any WASM module instances
    /// 2. Release any allocated resources
    /// 3. Finalize any pending checkpoints
    ///
    /// # Returns
    /// Ok(()) if cleanup succeeds, or an error if it fails
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    /// Start all output sinks
    ///
    /// This method should start all output sinks managed by the processor.
    ///
    /// # Returns
    /// Ok(()) if all sinks start successfully, or an error if any sink fails
    fn start_sinks(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        Ok(())
    }

    /// Stop all output sinks
    ///
    /// This method should stop all output sinks managed by the processor.
    ///
    /// # Returns
    /// Ok(()) if all sinks stop successfully, or an error if any sink fails
    fn stop_sinks(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        Ok(())
    }

    /// Take checkpoint for all output sinks
    ///
    /// This method should trigger checkpoint for all output sinks.
    ///
    /// # Arguments
    /// * `checkpoint_id` - Unique identifier for this checkpoint
    ///
    /// # Returns
    /// Ok(()) if all sinks checkpoint successfully, or an error if any sink fails
    fn take_checkpoint_sinks(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!("Taking checkpoint for sinks: {}", checkpoint_id);
        Ok(())
    }

    /// Finish checkpoint for all output sinks
    ///
    /// This method should finish checkpoint for all output sinks.
    ///
    /// # Arguments
    /// * `checkpoint_id` - Unique identifier for this checkpoint
    ///
    /// # Returns
    /// Ok(()) if all sinks finish checkpoint successfully, or an error if any sink fails
    fn finish_checkpoint_sinks(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        log::debug!("Finishing checkpoint for sinks: {}", checkpoint_id);
        Ok(())
    }

    /// Close all output sinks
    ///
    /// This method should close all output sinks managed by the processor.
    ///
    /// # Returns
    /// Ok(()) if all sinks close successfully, or an error if any sink fails
    fn close_sinks(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation: do nothing
        Ok(())
    }

    /// Get a mutable reference to the underlying Any type for downcasting
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
