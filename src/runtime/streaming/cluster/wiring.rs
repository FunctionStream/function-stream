//! 物理拓扑构建：channel 与一对一子任务边。
//!
//! 将 `arroyo_datastream::LogicalGraph` 完整编译为 Task 管道属于上层 worker/planner；
//! 此处提供 **与图无关** 的 channel 工厂与边展开，供适配层调用。

use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub type SubtaskKey = (String, u32);

pub type SubtaskOutChannels = HashMap<SubtaskKey, Vec<Sender<TrackedEvent>>>;
pub type SubtaskInChannels = HashMap<SubtaskKey, Vec<Receiver<TrackedEvent>>>;

pub fn stream_channel(capacity: usize) -> (Sender<TrackedEvent>, Receiver<TrackedEvent>) {
    mpsc::channel(capacity)
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct NodeSpec {
    pub id: String,
    pub parallelism: u32,
}

#[derive(Debug, Clone)]
pub struct PhysicalEdge {
    pub from: (String, u32),
    pub to: (String, u32),
}

/// 为每条 `PhysicalEdge` 建一条独立 channel，并挂到对应子任务的 sender/receiver 列表。
pub fn build_one_to_one_channels(
    edges: &[PhysicalEdge],
    capacity: usize,
) -> (SubtaskOutChannels, SubtaskInChannels) {
    let mut senders: SubtaskOutChannels = HashMap::new();
    let mut receivers: SubtaskInChannels = HashMap::new();

    for e in edges {
        let (tx, rx) = stream_channel(capacity);
        senders.entry(e.from.clone()).or_default().push(tx);
        receivers.entry(e.to.clone()).or_default().push(rx);
    }

    (senders, receivers)
}
