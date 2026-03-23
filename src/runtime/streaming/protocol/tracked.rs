use std::sync::Arc;

use crate::runtime::streaming::memory::MemoryTicket;
use crate::runtime::streaming::protocol::event::StreamEvent;

/// 在 Channel 中实际传输的事件，完美解决多路广播 (Broadcast) 的内存管理问题。
///
/// `MemoryTicket` 包在 `Arc` 中：如果 Event 被发送给 N 个下游分区（Broadcast 路由），
/// 只需 Clone 此 `TrackedEvent`，底层数据共享一块内存，Arc 引用计数 +N。
/// 只有当所有下游全部处理完并 Drop 后，Arc 归零，内存才被真正释放给 Pool。
#[derive(Debug, Clone)]
pub struct TrackedEvent {
    pub event: StreamEvent,
    pub _ticket: Option<Arc<MemoryTicket>>,
}

impl TrackedEvent {
    pub fn new(event: StreamEvent, ticket: Option<MemoryTicket>) -> Self {
        Self {
            event,
            _ticket: ticket.map(Arc::new),
        }
    }

    pub fn control(event: StreamEvent) -> Self {
        Self {
            event,
            _ticket: None,
        }
    }
}
