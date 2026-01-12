// StreamTaskInput - Stream task input interface
//
// Defines methods for reading data from input sources

use crate::runtime::io::{
    DataInputStatus, DataOutput, AvailabilityProvider,
};

/// PushingAsyncDataInput - Pushing async data input interface
/// 
/// Defines pushing async data input interface, unified handling of network input and source input
pub trait PushingAsyncDataInput: AvailabilityProvider {
    /// Push elements to output, return input status
    fn emit_next(&mut self, output: &mut dyn DataOutput) -> Result<DataInputStatus, Box<dyn std::error::Error + Send>>;
}

/// StreamTaskInput - Base interface for stream task input
/// 
/// Defines methods for reading data from input sources
pub trait StreamTaskInput: PushingAsyncDataInput + Send + Sync {
    /// Unspecified input index
    const UNSPECIFIED: i32 = -1;

    /// Return input index
    fn get_input_index(&self) -> i32;

    /// Prepare checkpoint snapshot
    /// 
    /// Returns a Future that completes when snapshot is ready
    fn prepare_snapshot(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;
}

/// CheckpointableInput - Checkpointable input
/// 
/// Input interface supporting checkpoint operations
pub trait CheckpointableInput: Send + Sync {
    /// Prepare checkpoint snapshot
    fn prepare_snapshot(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;
}

/// RecoverableStreamTaskInput - Recoverable stream task input
/// 
/// Input interface supporting recovery operations
/// 
/// Note: Due to Rust type system limitations, this trait cannot be directly used as a trait object
/// Actual implementation needs to handle based on specific types
pub trait RecoverableStreamTaskInput: StreamTaskInput {
    /// Finish recovery, switch to normal input
    /// 
    /// Note: Due to type erasure limitations, this method may need to recreate objects in actual implementation
    fn finish_recovery(self: Box<Self>) -> Result<(), Box<dyn std::error::Error + Send>>
    where
        Self: 'static;
}

