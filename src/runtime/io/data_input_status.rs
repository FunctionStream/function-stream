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

// DataInputStatus - Input status enumeration
//
// Represents the processing status of input data

/// DataInputStatus - Input status
///
/// Enumeration type representing the processing status of input data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataInputStatus {
    /// More data immediately available
    MoreAvailable,

    /// No data currently, but will be available in the future
    NothingAvailable,

    /// All persisted data has been successfully recovered
    EndOfRecovery,

    /// Input stopped (stop-with-savepoint without drain)
    Stopped,

    /// Input has reached end of data
    EndOfData,

    /// Input has reached end of data and control events, will close soon
    EndOfInput,
}

impl DataInputStatus {
    /// Check if more data is available
    pub fn has_more_available(&self) -> bool {
        matches!(self, DataInputStatus::MoreAvailable)
    }

    /// Check if waiting is needed
    pub fn needs_waiting(&self) -> bool {
        matches!(self, DataInputStatus::NothingAvailable)
    }

    /// Check if ended
    pub fn is_end(&self) -> bool {
        matches!(
            self,
            DataInputStatus::EndOfData | DataInputStatus::EndOfInput | DataInputStatus::Stopped
        )
    }

    /// Check if processing can continue
    pub fn can_continue(&self) -> bool {
        matches!(
            self,
            DataInputStatus::MoreAvailable | DataInputStatus::EndOfRecovery
        )
    }
}
