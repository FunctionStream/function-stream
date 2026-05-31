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

//! Delta Lake sink with Arroyo-style separation:
//! - [`delta_writer`]: [`DeltaSink`] / [`DeltaLocalSink`] write `part-*` data files
//! - [`delta_commit`]: checkpoint-time `_delta_log` transaction commit
//! - [`DeltaSinkOperator`]: stream operator coordinator

mod delta_commit;
mod delta_writer;

pub use delta_commit::strip_streaming_system_columns_arc;
pub use delta_writer::FinishedFile;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use arrow::compute::concat_batches;
use delta_commit::{
    DeltaCommitStrategy, DeltaTableCommitter, build_delta_storage_options, resolve_delta_table_uri,
};
use parquet::basic::Compression;
use tracing::{error, info, instrument, warn};
use url::Url;

use crate::core::StreamOutput;
use crate::core::api::context::TaskContext;
use crate::core::api::operator::{Collector, Operator};
use crate::memory::{MemoryBlock, try_global_memory_pool};
use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{CheckpointBarrier, Watermark};

use delta_writer::{DeltaLocalSink, DeltaSink, preflight_s3_bucket};

const DEFAULT_MAX_BUFFER_BYTES: usize = 256 * 1024 * 1024;

fn pending_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Merge many small stream batches into one Parquet row group where possible.
fn merge_pending_for_write(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, DeltaSinkError> {
    if batches.len() <= 1 {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)
        .map_err(|e| DeltaSinkError::Config(format!("failed to merge pending batches: {e}")))?;
    Ok(vec![merged])
}

fn ensure_delta_handlers_registered() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        deltalake::aws::register_handlers(None);
    });
}

#[derive(thiserror::Error, Debug)]
pub enum DeltaSinkError {
    #[error("local filesystem I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("serialization task panicked: {0}")]
    SerializationPanic(String),

    #[error("delta committer failed: {0}")]
    CommitterFailed(String),

    #[error("configuration error: {0}")]
    Config(String),
}

enum DeltaSinkBackend {
    Local(DeltaLocalSink),
    ObjectStore(DeltaSink),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaFormat {
    Csv,
    Parquet,
    JsonL,
    Avro,
    Orc,
}

/// Stream operator: buffers batches, delegates data-file writes to [`DeltaSinkBackend`],
/// and commits `_delta_log` at checkpoint via [`DeltaTableCommitter`].
pub struct DeltaSinkOperator {
    table_name: String,
    sink: DeltaSinkBackend,
    parquet_compression: Compression,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    sink_memory_block: Option<Arc<MemoryBlock>>,
    early_flush_threshold_bytes: usize,
    format: DeltaFormat,
    committer: Option<DeltaTableCommitter>,
    table_uri: Option<Url>,
    storage_options: HashMap<String, String>,
    commit_strategy: DeltaCommitStrategy,
    s3_bucket: Option<String>,
    sink_path: String,
    catalog_schema: Option<Arc<ArrowSchema>>,
    parquet_write_schema: Option<Arc<ArrowSchema>>,
    /// Retained for local path creation in [`Operator::on_start`].
    local_root: Option<PathBuf>,
}

impl DeltaSinkOperator {
    pub fn try_new(
        table_name: String,
        path: String,
        format: DeltaFormat,
        parquet_compression: Compression,
        sink_memory_bytes: u64,
        options: HashMap<String, String>,
        catalog_schema: Option<Arc<ArrowSchema>>,
    ) -> Result<Self, DeltaSinkError> {
        let s3_bucket = options.get(opt::S3_BUCKET).cloned();
        let (storage_options, commit_strategy) = build_delta_storage_options(&options)?;

        let (sink, local_root) = if let Some(bucket) = &s3_bucket {
            let s3 = DeltaSink::try_new(bucket.clone(), path.clone(), &options)?;
            (DeltaSinkBackend::ObjectStore(s3), None)
        } else {
            let root = PathBuf::from(path.clone());
            (
                DeltaSinkBackend::Local(DeltaLocalSink::new(root.clone())),
                Some(root),
            )
        };

        let mut sink_memory_block = None;
        let reserve_bytes = usize::try_from(sink_memory_bytes).unwrap_or(DEFAULT_MAX_BUFFER_BYTES);
        let mut early_flush_threshold_bytes = reserve_bytes;

        if let Ok(pool) = try_global_memory_pool() {
            if let Ok(block) = pool.try_request_block(reserve_bytes as u64) {
                early_flush_threshold_bytes = ((block.capacity() as usize) * 8) / 10;
                sink_memory_block = Some(block);
            }
        }

        Ok(Self {
            table_name,
            sink,
            parquet_compression,
            pending: Vec::with_capacity(64),
            pending_bytes: 0,
            sink_memory_block,
            early_flush_threshold_bytes,
            format,
            committer: None,
            table_uri: None,
            storage_options,
            commit_strategy,
            s3_bucket,
            sink_path: path,
            catalog_schema: catalog_schema.and_then(strip_streaming_system_columns_arc),
            parquet_write_schema: None,
            local_root,
        })
    }

    #[instrument(skip(self), fields(table = %self.table_name))]
    async fn flush_data_file(
        &mut self,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<(), DeltaSinkError> {
        if self.pending.is_empty() || pending_row_count(&self.pending) == 0 {
            self.pending.clear();
            self.pending_bytes = 0;
            return Ok(());
        }

        let fallback_schema = self.catalog_schema.is_none().then(|| {
            strip_streaming_system_columns_arc(self.pending[0].schema())
                .expect("batch has no user columns after removing streaming system columns")
        });

        let batches = merge_pending_for_write(std::mem::take(&mut self.pending))?;
        self.pending_bytes = 0;

        let compression = self.parquet_compression;
        let format = self.format;
        let schema = self.parquet_write_schema.clone();

        let finished = match &mut self.sink {
            DeltaSinkBackend::ObjectStore(sink) => {
                sink.write_batches(&batches, format, compression, schema, epoch, subtask_idx)
                    .await?
            }
            DeltaSinkBackend::Local(sink) => {
                sink.write_batches(&batches, format, compression, schema, epoch, subtask_idx)
                    .await?
            }
        };

        let Some(finished) = finished else {
            return Ok(());
        };

        if let Some(committer) = self.committer.as_mut() {
            if let Some(schema) = fallback_schema {
                committer.update_schema(schema)?;
            }
            committer.register_uncommitted(finished.into_uncommitted());
        }

        Ok(())
    }

    async fn flush_and_commit_checkpoint(
        &mut self,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<(), DeltaSinkError> {
        self.flush_data_file(epoch, subtask_idx).await?;

        if let Some(committer) = self.committer.as_mut() {
            if committer.has_uncommitted() {
                info!(
                    table = %self.table_name,
                    epoch,
                    subtask_idx,
                    "delta sink triggering checkpoint commit"
                );
                committer.commit_checkpoint(epoch).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Operator for DeltaSinkOperator {
    fn name(&self) -> &str {
        factory_operator_name::CONNECTOR_SINK
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        ensure_delta_handlers_registered();

        if let Some(root) = &self.local_root {
            tokio::fs::create_dir_all(root).await?;
        }

        if let DeltaSinkBackend::ObjectStore(sink) = &self.sink {
            preflight_s3_bucket(
                sink.client().as_ref(),
                sink.prefix_path(),
                self.s3_bucket.as_deref(),
            )
            .await?;
        }

        let table_uri = resolve_delta_table_uri(&self.sink_path, self.s3_bucket.as_deref())?;
        self.table_uri = Some(table_uri.clone());

        if self.format == DeltaFormat::Parquet {
            let committer = DeltaTableCommitter::try_new(
                table_uri,
                self.storage_options.clone(),
                self.catalog_schema.clone(),
            )?;
            self.parquet_write_schema = committer.write_schema();
            self.committer = Some(committer);
        } else {
            warn!(
                format = ?self.format,
                "format is not Parquet; writing raw files WITHOUT Delta transaction logs"
            );
        }

        info!(
            table = %self.table_name,
            threshold_bytes = self.early_flush_threshold_bytes,
            is_true_delta = self.committer.is_some(),
            commit_strategy = %self.commit_strategy.label(),
            "delta sink operator started (physical write + checkpoint commit)"
        );
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> anyhow::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.pending_bytes += batch.get_array_memory_size();
        self.pending.push(batch);

        if self.pending_bytes > self.early_flush_threshold_bytes {
            self.flush_data_file(0, ctx.subtask_index as usize).await?;
        }
        Ok(())
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[instrument(skip(self, ctx), fields(epoch = barrier.epoch))]
    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        self.flush_and_commit_checkpoint(barrier.epoch, ctx.subtask_index as usize)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> anyhow::Result<Vec<StreamOutput>> {
        if !self.pending.is_empty() || self.committer.as_ref().is_some_and(|c| c.has_uncommitted())
        {
            warn!(
                table = %self.table_name,
                "operator closing, forcing final flush and commit"
            );
            if let Err(e) = self
                .flush_and_commit_checkpoint(u64::MAX, ctx.subtask_index as usize)
                .await
            {
                error!(
                    table = %self.table_name,
                    error = %e,
                    "fatal: final flush/commit on close failed; data may be lost"
                );
                return Err(e.into());
            }
        }
        Ok(vec![])
    }
}
