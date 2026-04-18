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

use super::block::MemoryBlock;

#[derive(Debug)]
pub struct MemoryTicket {
    bytes: u64,
    block: Arc<MemoryBlock>,
}

impl MemoryTicket {
    pub(crate) fn new(bytes: u64, block: Arc<MemoryBlock>) -> Self {
        Self { bytes, block }
    }

    #[inline]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryTicket {
    fn drop(&mut self) {
        self.block.release_ticket(self.bytes);
    }
}
