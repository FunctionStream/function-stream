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

// StreamInputProcessor - Stream input processor
//
// Defines standard methods for processing input data
//
// Note: This implementation only supports multi-input, single-input scenarios should use multi-input processor (with only one input)

use crate::runtime::io::{AvailabilityProvider, DataInputStatus};

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
    fn prepare_snapshot(&self, checkpoint_id: u64)
    -> Result<(), Box<dyn std::error::Error + Send>>;

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
