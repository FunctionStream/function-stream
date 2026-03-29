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

use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::ticket::MemoryTicket;

#[derive(Debug)]
pub struct MemoryPool {
    max_bytes: usize,
    used_bytes: AtomicUsize,
    available_bytes: Mutex<usize>,
    notify: Notify,
}

impl MemoryPool {
    pub fn new(max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_bytes,
            used_bytes: AtomicUsize::new(0),
            available_bytes: Mutex::new(max_bytes),
            notify: Notify::new(),
        })
    }

    pub fn usage_metrics(&self) -> (usize, usize) {
        (self.used_bytes.load(Ordering::Relaxed), self.max_bytes)
    }

    pub async fn request_memory(self: &Arc<Self>, bytes: usize) -> MemoryTicket {
        if bytes == 0 {
            return MemoryTicket::new(0, self.clone());
        }

        if bytes > self.max_bytes {
            warn!(
                "Requested memory ({} B) exceeds total pool size ({} B)! \
                Permitting to avoid pipeline deadlock, but OOM risk is critical.",
                bytes, self.max_bytes
            );
            self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
            return MemoryTicket::new(bytes, self.clone());
        }

        loop {
            {
                let mut available = self.available_bytes.lock();
                if *available >= bytes {
                    *available -= bytes;
                    self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
                    return MemoryTicket::new(bytes, self.clone());
                }
            }

            debug!("Backpressure engaged: waiting for {} bytes to be freed...", bytes);
            self.notify.notified().await;
        }
    }

    pub(crate) fn release(&self, bytes: usize) {
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
