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

use super::endpoint::{BoxedEventStream, PhysicalSender};
use std::collections::HashMap;

pub type VertexId = u32;
pub type SubtaskIndex = u32;

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
