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

use arrow_array::RecordBatch;

mod block;
mod error;
pub mod global;
pub mod pool;
pub mod ticket;

#[allow(unused_imports)]
pub use block::MemoryBlock;
#[allow(unused_imports)]
pub use error::{MemoryAllocationError, MemoryError};
#[allow(unused_imports)]
pub use global::{
    get_memory_metrics, global_memory_pool, init_global_memory_pool, try_global_memory_pool,
};
pub use pool::MemoryPool;
pub use ticket::MemoryTicket;

#[inline]
pub fn get_array_memory_size(batch: &RecordBatch) -> u64 {
    RecordBatch::get_array_memory_size(batch) as u64
}
