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
