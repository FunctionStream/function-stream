use std::sync::Arc;

use protocol::function_stream_graph::FsProgram;
use protocol::storage as pb;

use crate::CatalogResult;

/// One persisted streaming job row from catalog storage.
///
/// This is intentionally storage-agnostic: the catalog keeps source checkpoint
/// payloads as protocol oneof envelopes and does not inspect source-specific
/// checkpoint data.
#[derive(Debug, Clone)]
pub struct StoredStreamingJob {
    pub table_name: String,
    pub program: FsProgram,
    pub checkpoint_interval_ms: u64,
    pub latest_checkpoint_epoch: u64,
    pub source_checkpoints: Vec<pb::SourceCheckpointInfo>,
}

/// Interface exposed by the stream catalog manager.
///
/// The concrete table and planning types are generic so this crate can define
/// the catalog boundary without depending on the monolithic SQL/runtime crates.
pub trait StreamCatalog<Table, PlanningContext, StreamTable>: Send + Sync {
    fn persist_streaming_job(
        &self,
        table_name: &str,
        fs_program: &FsProgram,
        comment: &str,
        checkpoint_interval_ms: u64,
    ) -> CatalogResult<()>;

    fn remove_streaming_job(&self, table_name: &str) -> CatalogResult<()>;

    fn commit_job_checkpoint(
        &self,
        table_name: &str,
        epoch: u64,
        source_checkpoints: Vec<pb::SourceCheckpointInfo>,
    ) -> CatalogResult<()>;

    fn load_streaming_job_definitions(&self) -> CatalogResult<Vec<StoredStreamingJob>>;

    fn add_catalog_table(&self, table: Table) -> CatalogResult<()>;
    fn has_catalog_table(&self, name: &str) -> bool;
    fn drop_catalog_table(&self, table_name: &str, if_exists: bool) -> CatalogResult<()>;
    fn restore_from_store(&self) -> CatalogResult<()>;
    fn acquire_planning_context(&self) -> PlanningContext;
    fn list_catalog_tables(&self) -> CatalogResult<Vec<Arc<Table>>>;
    fn get_catalog_table(&self, name: &str) -> CatalogResult<Option<Arc<Table>>>;

    fn add_table(&self, table: StreamTable) -> CatalogResult<()>;
    fn has_stream_table(&self, name: &str) -> bool;
    fn drop_table(&self, table_name: &str, if_exists: bool) -> CatalogResult<()>;
    fn list_stream_tables(&self) -> Vec<Arc<StreamTable>>;
    fn get_stream_table(&self, name: &str) -> Option<Arc<StreamTable>>;
}

/// Process-global catalog access boundary.
pub trait GlobalStreamCatalog<Manager>: Send + Sync {
    fn init_global(manager: Arc<Manager>) -> CatalogResult<()>;
    fn try_global() -> Option<Arc<Manager>>;

    fn global() -> CatalogResult<Arc<Manager>> {
        Self::try_global().ok_or_else(|| "CatalogManager not initialized".into())
    }
}
