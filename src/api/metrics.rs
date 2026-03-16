use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricName {
    BytesRecv,
    BytesSent,
    MessagesRecv,
    MessagesSent,
    Backpressure,
    TxQueueSize,
    TxQueueRem,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Metric {
    pub time: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct SubtaskMetrics {
    pub index: u32,
    pub metrics: Vec<Metric>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct MetricGroup {
    pub name: MetricName,
    pub subtasks: Vec<SubtaskMetrics>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct OperatorMetricGroup {
    pub node_id: u32,
    pub metric_groups: Vec<MetricGroup>,
}
