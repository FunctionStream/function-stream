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

use crate::runtime::streaming::memory::MemoryTicket;
use crate::runtime::streaming::protocol::event::StreamEvent;

///
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
