// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryError {
    AlreadyInitialized,
    Uninitialized,
    OsAllocationFailed { bytes: u64 },
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryError::AlreadyInitialized => {
                write!(f, "Global memory pool is already initialized")
            }
            MemoryError::Uninitialized => {
                write!(f, "Global memory pool is not initialized")
            }
            MemoryError::OsAllocationFailed { bytes } => {
                write!(
                    f,
                    "insufficient memory: failed to reserve {} bytes (virtual capacity for pool cap) from the OS allocator",
                    bytes
                )
            }
        }
    }
}

impl std::error::Error for MemoryError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAllocationError {
    InsufficientCapacity,
    RequestLargerThanPool,
}

impl fmt::Display for MemoryAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryAllocationError::InsufficientCapacity => {
                write!(f, "Insufficient capacity in memory pool")
            }
            MemoryAllocationError::RequestLargerThanPool => {
                write!(f, "Requested block exceeds memory pool maximum")
            }
        }
    }
}

impl std::error::Error for MemoryAllocationError {}
