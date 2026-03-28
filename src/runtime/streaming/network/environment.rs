use super::endpoint::{BoxedEventStream, PhysicalSender};
use std::collections::HashMap;

pub type VertexId = u32;
pub type SubtaskIndex = u32;

/// 物理网络路由注册表
pub struct NetworkEnvironment {
    pub outboxes: HashMap<(VertexId, SubtaskIndex), Vec<PhysicalSender>>,
    pub inboxes: HashMap<(VertexId, SubtaskIndex), Vec<BoxedEventStream>>,
}

impl NetworkEnvironment {
    pub fn new() -> Self {
        Self {
            outboxes: HashMap::new(),
            inboxes: HashMap::new(),
        }
    }

    pub fn take_outboxes(
        &mut self,
        vertex_id: VertexId,
        subtask_idx: SubtaskIndex,
    ) -> Vec<PhysicalSender> {
        self.outboxes
            .remove(&(vertex_id, subtask_idx))
            .unwrap_or_default()
    }

    pub fn take_inboxes(
        &mut self,
        vertex_id: VertexId,
        subtask_idx: SubtaskIndex,
    ) -> Vec<BoxedEventStream> {
        self.inboxes
            .remove(&(vertex_id, subtask_idx))
            .unwrap_or_default()
    }
}
