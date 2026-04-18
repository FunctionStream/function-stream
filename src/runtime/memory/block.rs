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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::pool::MemoryPool;
use super::ticket::MemoryTicket;

#[derive(Debug)]
pub struct MemoryBlock {
    capacity: u64,
    available_bytes: AtomicU64,
    pool: Arc<MemoryPool>,
}

impl MemoryBlock {
    pub(crate) fn new(capacity: u64, pool: Arc<MemoryPool>) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            available_bytes: AtomicU64::new(capacity),
            pool,
        })
    }

    pub fn try_allocate(self: &Arc<Self>, bytes: u64) -> Option<MemoryTicket> {
        if bytes == 0 {
            return Some(MemoryTicket::new(0, self.clone()));
        }

        let mut current_available = self.available_bytes.load(Ordering::Acquire);
        loop {
            if current_available < bytes {
                return None;
            }

            match self.available_bytes.compare_exchange_weak(
                current_available,
                current_available - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(MemoryTicket::new(bytes, self.clone())),
                Err(actual) => current_available = actual,
            }
        }
    }

    #[inline]
    pub fn available_bytes(&self) -> u64 {
        self.available_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn release_ticket(&self, bytes: u64) {
        if bytes > 0 {
            self.available_bytes.fetch_add(bytes, Ordering::Release);
        }
    }
}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        self.pool.release_block(self.capacity);
    }
}
