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

pub fn init_global_memory_pool(max_bytes: u64) -> Result<(), MemoryError> {
    let pool = MemoryPool::try_new(max_bytes)?;
    GLOBAL_POOL
        .set(pool)
        .map_err(|_| MemoryError::AlreadyInitialized)
}

pub fn try_global_memory_pool() -> Result<Arc<MemoryPool>, MemoryError> {
    GLOBAL_POOL.get().cloned().ok_or(MemoryError::Uninitialized)
}

#[inline]
pub fn global_memory_pool() -> Arc<MemoryPool> {
    try_global_memory_pool().expect("Global memory pool must be initialized before use")
}

pub fn get_memory_metrics() -> Option<(u64, u64)> {
    GLOBAL_POOL.get().map(|p| p.usage_metrics())
}
