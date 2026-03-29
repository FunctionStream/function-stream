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

use std::sync::Arc;

use super::pool::MemoryPool;

/// 内存船票 (RAII Guard)
/// 不实现 Clone：生命周期严格对应唯一的字节扣减。
/// 跨多路广播时应包裹在 `Arc<MemoryTicket>` 中。
#[derive(Debug)]
pub struct MemoryTicket {
    bytes: usize,
    pool: Arc<MemoryPool>,
}

impl MemoryTicket {
    pub(crate) fn new(bytes: usize, pool: Arc<MemoryPool>) -> Self {
        Self { bytes, pool }
    }
}

impl Drop for MemoryTicket {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}
