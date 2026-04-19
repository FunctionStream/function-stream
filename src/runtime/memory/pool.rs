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

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::block::MemoryBlock;
use super::error::{MemoryAllocationError, MemoryError};

#[derive(Debug)]
pub struct MemoryPool {
    max_bytes: u64,
    used_bytes: AtomicU64,
    available_bytes: Mutex<u64>,
    notify: Notify,
}

impl MemoryPool {
    pub fn try_new(max_bytes: u64) -> Result<Arc<Self>, MemoryError> {
        if max_bytes > 0 {
            let n = usize::try_from(max_bytes)
                .map_err(|_| MemoryError::OsAllocationFailed { bytes: max_bytes })?;
            let mut v = Vec::<u8>::new();
            v.try_reserve_exact(n)
                .map_err(|_| MemoryError::OsAllocationFailed { bytes: max_bytes })?;
        }
        Ok(Arc::new(Self {
            max_bytes,
            used_bytes: AtomicU64::new(0),
            available_bytes: Mutex::new(max_bytes),
            notify: Notify::new(),
        }))
    }

    pub fn new(max_bytes: u64) -> Arc<Self> {
        Self::try_new(max_bytes).expect("MemoryPool::try_new failed")
    }

    pub fn usage_metrics(&self) -> (u64, u64) {
        (self.used_bytes.load(Ordering::Relaxed), self.max_bytes)
    }

    pub fn try_request_block(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<Arc<MemoryBlock>, MemoryAllocationError> {
        if bytes == 0 {
            return Ok(MemoryBlock::new(0, self.clone()));
        }
        if bytes > self.max_bytes {
            return Err(MemoryAllocationError::RequestLargerThanPool);
        }
        let mut available = self.available_bytes.lock();
        if *available >= bytes {
            *available -= bytes;
            self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
            Ok(MemoryBlock::new(bytes, self.clone()))
        } else {
            Err(MemoryAllocationError::InsufficientCapacity)
        }
    }

    pub async fn request_block(self: &Arc<Self>, bytes: u64) -> Arc<MemoryBlock> {
        if bytes == 0 {
            return MemoryBlock::new(0, self.clone());
        }

        if bytes > self.max_bytes {
            warn!(
                request_bytes = bytes,
                max_bytes = self.max_bytes,
                "Requested memory block exceeds total pool size! \
                Permitting to avoid pipeline deadlock, but critical OOM risk exists."
            );
            self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
            return MemoryBlock::new(bytes, self.clone());
        }

        loop {
            {
                let mut available = self.available_bytes.lock();
                if *available >= bytes {
                    *available -= bytes;
                    self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
                    return MemoryBlock::new(bytes, self.clone());
                }
            }

            debug!(
                bytes = bytes,
                "Global backpressure engaged: waiting for memory..."
            );
            self.notify.notified().await;
        }
    }

    pub fn force_reserve(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut available = self.available_bytes.lock();
        *available = available.saturating_sub(bytes);
        self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn force_release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.release_block(bytes);
    }

    pub(crate) fn release_block(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        {
            let mut available = self.available_bytes.lock();
            *available += bytes;
        }

        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}
