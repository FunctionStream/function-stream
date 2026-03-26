use std::collections::HashMap;

use protocol::grpc::api::{FsEdge, FsNode};
use tokio::sync::mpsc;

use crate::runtime::streaming::protocol::tracked::TrackedEvent;

pub struct EdgeManager {
    // PipelineID -> (输入 Receiver, 输出 Sender 列表)
    endpoints: HashMap<u32, (Option<mpsc::Receiver<TrackedEvent>>, Vec<mpsc::Sender<TrackedEvent>>)>,
}

impl EdgeManager {
    pub fn build(nodes: &[FsNode], edges: &[FsEdge]) -> Self {
        let mut tx_map: HashMap<u32, Vec<mpsc::Sender<TrackedEvent>>> = HashMap::new();
        let mut rx_map: HashMap<u32, mpsc::Receiver<TrackedEvent>> = HashMap::new();

        for edge in edges {
            let (tx, rx) = mpsc::channel(2048);
            tx_map.entry(edge.source as u32).or_default().push(tx);
            rx_map.insert(edge.target as u32, rx);
        }

        let mut endpoints = HashMap::new();
        for node in nodes {
            let id = node.node_index as u32;
            endpoints.insert(id, (rx_map.remove(&id), tx_map.remove(&id).unwrap_or_default()));
        }

        Self { endpoints }
    }

    pub fn take_endpoints(
        &mut self,
        id: u32,
    ) -> (Option<mpsc::Receiver<TrackedEvent>>, Vec<mpsc::Sender<TrackedEvent>>) {
        self.endpoints
            .remove(&id)
            .expect("Critical: Execution Graph Inconsistent")
    }
}
