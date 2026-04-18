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

use std::sync::{Arc, OnceLock};

use super::error::MemoryError;
use super::pool::MemoryPool;

static GLOBAL_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();
static GLOBAL_STATE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();

pub fn init_global_memory_pool(max_bytes: u64) -> Result<(), MemoryError> {
    GLOBAL_POOL
        .set(MemoryPool::new(max_bytes))
        .map_err(|_| MemoryError::AlreadyInitialized)
}

pub fn try_global_memory_pool() -> Result<Arc<MemoryPool>, MemoryError> {
    GLOBAL_POOL.get().cloned().ok_or(MemoryError::Uninitialized)
}

#[inline]
pub fn global_memory_pool() -> Arc<MemoryPool> {
    try_global_memory_pool().expect("Global streaming pool must be initialized before use")
}

pub fn init_global_state_memory_pool(max_bytes: u64) -> Result<(), MemoryError> {
    GLOBAL_STATE_POOL
        .set(MemoryPool::new(max_bytes))
        .map_err(|_| MemoryError::AlreadyInitialized)
}

pub fn try_global_state_memory_pool() -> Result<Arc<MemoryPool>, MemoryError> {
    GLOBAL_STATE_POOL.get().cloned().ok_or(MemoryError::Uninitialized)
}

#[inline]
pub fn global_state_memory_pool() -> Arc<MemoryPool> {
    try_global_state_memory_pool().expect("Global state pool must be initialized before use")
}

pub fn get_memory_metrics() -> (Option<(u64, u64)>, Option<(u64, u64)>) {
    let stream_metrics = GLOBAL_POOL.get().map(|p| p.usage_metrics());
    let state_metrics = GLOBAL_STATE_POOL.get().map(|p| p.usage_metrics());
    (stream_metrics, state_metrics)
}
