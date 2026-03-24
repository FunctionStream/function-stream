use anyhow::Result;
use async_trait::async_trait;

#[derive(Default, Debug, Clone)]
pub struct CheckpointMetadata {
    pub job_id: String,
    pub epoch: u32,
    pub min_epoch: u32,
    pub operator_ids: Vec<String>,
}

#[derive(Default, Debug, Clone)]
pub struct OperatorCheckpointMetadata {
    pub operator_id: String,
    pub epoch: u32,
}

#[async_trait]
pub trait BackingStore: Send + Sync + 'static {
    fn name() -> &'static str;
    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Result<CheckpointMetadata>;
    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>>;
    async fn write_operator_checkpoint_metadata(
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()>;
    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) -> Result<()>;
    async fn cleanup_checkpoint(
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()>;
}

pub struct ParquetStateBackend;

#[async_trait]
impl BackingStore for ParquetStateBackend {
    fn name() -> &'static str {
        "parquet"
    }

    async fn load_checkpoint_metadata(
        _job_id: &str,
        _epoch: u32,
    ) -> Result<CheckpointMetadata> {
        Ok(CheckpointMetadata::default())
    }

    async fn load_operator_metadata(
        _job_id: &str,
        _operator_id: &str,
        _epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>> {
        Ok(None)
    }

    async fn write_operator_checkpoint_metadata(
        _metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        Ok(())
    }

    async fn write_checkpoint_metadata(_metadata: CheckpointMetadata) -> Result<()> {
        Ok(())
    }

    async fn cleanup_checkpoint(
        _metadata: CheckpointMetadata,
        _old_min_epoch: u32,
        _new_min_epoch: u32,
    ) -> Result<()> {
        Ok(())
    }
}
