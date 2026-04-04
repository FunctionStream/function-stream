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

use anyhow::{Result, anyhow};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::runtime::streaming::protocol::event::TrackedEvent;
use protocol::grpc::api::{FsEdge, FsNode};

const DEFAULT_CHANNEL_CAPACITY: usize = 2048;

type TrackedEventEndpoints = (
    Vec<mpsc::Receiver<TrackedEvent>>,
    Vec<mpsc::Sender<TrackedEvent>>,
);

pub struct EdgeManager {
    endpoints: HashMap<u32, TrackedEventEndpoints>,
}

impl EdgeManager {
    pub fn build(nodes: &[FsNode], edges: &[FsEdge]) -> Self {
        Self::build_with_capacity(nodes, edges, DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn build_with_capacity(nodes: &[FsNode], edges: &[FsEdge], capacity: usize) -> Self {
        info!(
            "Building EdgeManager for {} nodes and {} edges (channel capacity: {})",
            nodes.len(),
            edges.len(),
            capacity
        );

        let mut tx_map: HashMap<u32, Vec<mpsc::Sender<TrackedEvent>>> =
            HashMap::with_capacity(nodes.len());
        let mut rx_map: HashMap<u32, Vec<mpsc::Receiver<TrackedEvent>>> =
            HashMap::with_capacity(nodes.len());

        for edge in edges {
            let source_id = edge.source as u32;
            let target_id = edge.target as u32;

            let (tx, rx) = mpsc::channel(capacity);

            tx_map.entry(source_id).or_default().push(tx);
            rx_map.entry(target_id).or_default().push(rx);

            debug!(
                "Created physical edge channel: Node {} -> Node {}",
                source_id, target_id
            );
        }

        let mut endpoints = HashMap::with_capacity(nodes.len());
        for node in nodes {
            let id = node.node_index as u32;

            let inboxes = rx_map.remove(&id).unwrap_or_default();
            let outboxes = tx_map.remove(&id).unwrap_or_default();

            endpoints.insert(id, (inboxes, outboxes));
        }

        for remaining_target in rx_map.keys() {
            warn!(
                "Topology Warning: Found incoming edges pointing to non-existent Node {}",
                remaining_target
            );
        }
        for remaining_source in tx_map.keys() {
            warn!(
                "Topology Warning: Found outgoing edges coming from non-existent Node {}",
                remaining_source
            );
        }

        Self { endpoints }
    }

    pub fn take_endpoints(&mut self, id: u32) -> Result<TrackedEventEndpoints> {
        self.endpoints
            .remove(&id)
            .ok_or_else(|| anyhow!(
                "Topology Error: Endpoints for Node {} not found or already taken. Execution Graph may be inconsistent.",
                id
            ))
    }
}
