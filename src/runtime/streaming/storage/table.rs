use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Default)]
pub struct TaskInfo {
    pub job_id: String,
    pub operator_id: String,
    pub task_index: u32,
}

#[derive(Debug)]
pub enum TableData {
    RecordBatch(RecordBatch),
    CommitData { data: Vec<u8> },
    KeyedData { key: Vec<u8>, value: Vec<u8> },
}

pub struct CheckpointMessage {
    pub epoch: u32,
    pub time: std::time::SystemTime,
    pub watermark: Option<std::time::SystemTime>,
    pub then_stop: bool,
}

#[async_trait]
pub trait TableEpochCheckpointer: Send + 'static {
    type SubTableCheckpointMessage: prost::Message + Default;

    async fn insert_data(&mut self, _data: TableData) -> Result<()> {
        Ok(())
    }

    async fn finish(
        self: Box<Self>,
        _checkpoint: &CheckpointMessage,
    ) -> Result<Option<(Self::SubTableCheckpointMessage, usize)>> {
        Ok(None)
    }

    fn subtask_index(&self) -> u32;
}

#[async_trait]
pub trait Table: Send + Sync + 'static + Clone {
    type Checkpointer: TableEpochCheckpointer<
        SubTableCheckpointMessage = Self::TableSubtaskCheckpointMetadata,
    >;
    type ConfigMessage: prost::Message + Default;
    type TableCheckpointMessage: prost::Message + Default + Clone;
    type TableSubtaskCheckpointMetadata: prost::Message + Default + Clone;

    fn from_config(
        _config: Self::ConfigMessage,
        _task_info: Arc<TaskInfo>,
        _storage_provider: super::StorageProviderRef,
        _checkpoint_message: Option<Self::TableCheckpointMessage>,
        _state_version: u32,
    ) -> Result<Self>
    where
        Self: Sized;

    fn epoch_checkpointer(
        &self,
        _epoch: u32,
        _previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer>;

    fn merge_checkpoint_metadata(
        _config: Self::ConfigMessage,
        _subtask_metadata: HashMap<u32, Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<Self::TableCheckpointMessage>> {
        Ok(None)
    }

    fn subtask_metadata_from_table(
        &self,
        _table_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableSubtaskCheckpointMetadata>> {
        Ok(None)
    }

    fn files_to_keep(
        _config: Self::ConfigMessage,
        _checkpoint: Self::TableCheckpointMessage,
    ) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }
}
