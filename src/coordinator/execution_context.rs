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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static EXECUTION_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct ExecutionContext {
    pub execution_id: u64,
    pub start_time: Instant,
    pub timeout: Duration,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            execution_id: EXECUTION_ID_GENERATOR.fetch_add(1, Ordering::SeqCst),
            start_time: Instant::now(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn is_timeout(&self) -> bool {
        self.elapsed() >= self.timeout
    }

    pub fn remaining_timeout(&self) -> Duration {
        self.timeout.saturating_sub(self.elapsed())
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}
