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

use crossbeam_channel::TrySendError;
use thiserror::Error;

use crate::runtime::memory::MemoryAllocationError;

#[derive(Error, Debug)]
pub enum StateEngineError {
    #[error("I/O error during state persistence: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Parquet serialization/deserialization failed: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Arrow computation failed: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Memory hard limit exceeded and spill channel is full")]
    MemoryBackpressureTimeout,

    #[error("Background I/O pool has been shut down or disconnected")]
    IoPoolDisconnected,

    #[error("State metadata corrupted: {0}")]
    Corruption(String),

    #[error("State memory block reservation failed: {0}")]
    MemoryReservation(#[from] MemoryAllocationError),
}

pub type Result<T> = std::result::Result<T, StateEngineError>;

impl<T> From<TrySendError<T>> for StateEngineError {
    fn from(err: TrySendError<T>) -> Self {
        match err {
            TrySendError::Full(_) => StateEngineError::MemoryBackpressureTimeout,
            TrySendError::Disconnected(_) => StateEngineError::IoPoolDisconnected,
        }
    }
}
