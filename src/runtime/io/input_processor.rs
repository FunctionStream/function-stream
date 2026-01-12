// StreamInputProcessor - Stream input processor
//
// Defines standard methods for processing input data
// 
// Note: This implementation only supports multi-input, single-input scenarios should use multi-input processor (with only one input)

use crate::runtime::io::{
    DataInputStatus, AvailabilityProvider,
};

/// StreamInputProcessor - Core interface for stream task input processor
/// 
/// Defines standard methods for processing input data
/// 
/// Note: This implementation only supports multi-input scenarios, single input should use multi-input processor (with only one input)
pub trait StreamInputProcessor: AvailabilityProvider + Send + Sync {
    /// Process input data
    /// 
    /// Returns input status indicating whether more data is available for processing
    fn process_input(&mut self) -> Result<DataInputStatus, Box<dyn std::error::Error + Send>>;

    /// Prepare checkpoint snapshot
    /// 
    /// Returns a Future that completes when the snapshot is ready
    fn prepare_snapshot(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Close the input processor
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Default implementation is no-op
        Ok(())
    }

    /// Check if approximately available (for optimization, to avoid frequent volatile checks)
    fn is_approximately_available(&self) -> bool {
        // Default implementation calls is_available()
        self.is_available()
    }
}

/// BoundedMultiInput - Bounded multi-input aware interface
/// 
/// Used to notify bounded input end
pub trait BoundedMultiInput: Send + Sync {
    /// Notify input end
    fn end_input(&self, input_index: i32);
}

