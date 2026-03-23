use crate::runtime::streaming::cluster::graph::{
    ExchangeMode, ExecutionGraph, SubtaskIndex, VertexId,
};
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use super::endpoint::{BoxedEventStream, PhysicalSender, RemoteSenderStub};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

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

    pub fn build_from_graph(graph: &ExecutionGraph, local_queue_size: usize) -> Self {
        let mut env = Self::new();

        for edge in &graph.edges {
            let src_key = (edge.src_vertex, edge.src_subtask);
            let dst_key = (edge.dst_vertex, edge.dst_subtask);

            match &edge.exchange_mode {
                ExchangeMode::LocalThread => {
                    let (tx, rx) = mpsc::channel::<TrackedEvent>(local_queue_size);

                    let sender = PhysicalSender::Local(tx);
                    let receiver_stream =
                        Box::pin(ReceiverStream::new(rx)) as BoxedEventStream;

                    env.outboxes.entry(src_key).or_default().push(sender);
                    env.inboxes.entry(dst_key).or_default().push(receiver_stream);
                }
                ExchangeMode::RemoteNetwork { target_addr } => {
                    let remote_stub = RemoteSenderStub {
                        target_addr: target_addr.clone(),
                    };
                    env.outboxes
                        .entry(src_key)
                        .or_default()
                        .push(PhysicalSender::Remote(remote_stub));
                }
            }
        }

        info!(
            "Network Environment built. Wired {} connections.",
            graph.edges.len()
        );

        env
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
