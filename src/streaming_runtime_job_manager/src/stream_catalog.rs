use std::sync::{Arc, OnceLock};

use anyhow::{Result, anyhow};
use protocol::storage::SourceCheckpointInfo;

pub trait CheckpointCatalog: Send + Sync {
    fn commit_job_checkpoint(
        &self,
        job_id: &str,
        epoch: u64,
        source_infos: Vec<SourceCheckpointInfo>,
    ) -> Result<()>;
}

static GLOBAL_CHECKPOINT_CATALOG: OnceLock<Arc<dyn CheckpointCatalog>> = OnceLock::new();

pub fn install_global_checkpoint_catalog(catalog: Arc<dyn CheckpointCatalog>) -> Result<()> {
    GLOBAL_CHECKPOINT_CATALOG
        .set(catalog)
        .map_err(|_| anyhow!("CheckpointCatalog singleton already initialized"))
}

#[derive(Clone)]
pub struct CatalogHandle {
    inner: Arc<dyn CheckpointCatalog>,
}

pub struct CatalogManager;

impl CatalogManager {
    pub fn try_global() -> Option<CatalogHandle> {
        GLOBAL_CHECKPOINT_CATALOG
            .get()
            .cloned()
            .map(|inner| CatalogHandle { inner })
    }
}

impl CatalogHandle {
    pub fn commit_job_checkpoint(
        &self,
        job_id: &str,
        epoch: u64,
        source_infos: Vec<SourceCheckpointInfo>,
    ) -> Result<()> {
        self.inner
            .commit_job_checkpoint(job_id, epoch, source_infos)
    }
}
