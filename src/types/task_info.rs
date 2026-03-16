use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;

#[derive(Eq, PartialEq, Hash, Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct TaskInfo {
    pub job_id: String,
    pub node_id: u32,
    pub operator_name: String,
    pub operator_id: String,
    pub task_index: u32,
    pub parallelism: u32,
    pub key_range: RangeInclusive<u64>,
}

impl Display for TaskInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task_{}-{}/{}",
            self.operator_id, self.task_index, self.parallelism
        )
    }
}

impl TaskInfo {
    pub fn for_test(job_id: &str, operator_id: &str) -> Self {
        Self {
            job_id: job_id.to_string(),
            node_id: 1,
            operator_name: "op".to_string(),
            operator_id: operator_id.to_string(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
        }
    }
}

pub fn get_test_task_info() -> TaskInfo {
    TaskInfo {
        job_id: "instance-1".to_string(),
        node_id: 1,
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-1".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: 0..=u64::MAX,
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ChainInfo {
    pub job_id: String,
    pub node_id: u32,
    pub description: String,
    pub task_index: u32,
}

impl Display for ChainInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TaskChain{}-{} ({})",
            self.node_id, self.task_index, self.description
        )
    }
}

impl ChainInfo {
    pub fn metric_label_map(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("node_id".to_string(), self.node_id.to_string());
        labels.insert("subtask_idx".to_string(), self.task_index.to_string());
        labels.insert("node_description".to_string(), self.description.to_string());
        labels
    }
}
