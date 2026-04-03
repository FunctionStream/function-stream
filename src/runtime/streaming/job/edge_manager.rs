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

use std::collections::HashMap;

use protocol::grpc::api::{FsEdge, FsNode};
use tokio::sync::mpsc;

use crate::runtime::streaming::protocol::event::TrackedEvent;

pub struct EdgeManager {
    endpoints: HashMap<
        u32,
        (
            Vec<mpsc::Receiver<TrackedEvent>>,
            Vec<mpsc::Sender<TrackedEvent>>,
        ),
    >,
}

impl EdgeManager {
    pub fn build(nodes: &[FsNode], edges: &[FsEdge]) -> Self {
        let mut tx_map: HashMap<u32, Vec<mpsc::Sender<TrackedEvent>>> = HashMap::new();
        let mut rx_map: HashMap<u32, Vec<mpsc::Receiver<TrackedEvent>>> = HashMap::new();

        for edge in edges {
            let (tx, rx) = mpsc::channel(2048);
            tx_map.entry(edge.source as u32).or_default().push(tx);
            rx_map.entry(edge.target as u32).or_default().push(rx);
        }

        let mut endpoints = HashMap::new();
        for node in nodes {
            let id = node.node_index as u32;
            let inboxes = rx_map.remove(&id).unwrap_or_default();
            endpoints.insert(id, (inboxes, tx_map.remove(&id).unwrap_or_default()));
        }

        Self { endpoints }
    }

    pub fn take_endpoints(
        &mut self,
        id: u32,
    ) -> (
        Vec<mpsc::Receiver<TrackedEvent>>,
        Vec<mpsc::Sender<TrackedEvent>>,
    ) {
        self.endpoints
            .remove(&id)
            .expect("Critical: Execution Graph Inconsistent")
    }
}
