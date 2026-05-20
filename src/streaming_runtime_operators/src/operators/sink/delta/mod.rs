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

mod delta_commit;

pub use delta_commit::strip_streaming_system_columns_arc;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use bytes::Bytes;
use delta_commit::{
    DeltaCommitStrategy, DeltaTableCommitter, UncommittedDataFile, build_delta_storage_options,
    cast_batches_for_delta_write, resolve_delta_table_uri,
};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, PutPayload};
use parquet::basic::Compression;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument, warn};
use url::Url;

use crate::core::StreamOutput;
use crate::core::api::context::TaskContext;
use crate::core::api::operator::{Collector, Operator};
use crate::format::encoder::FormatEncoder;
use crate::memory::{MemoryBlock, try_global_memory_pool};
use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{CheckpointBarrier, Watermark};

const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Registers deltalake protocol handlers (e.g. `s3://`) exactly once per process.
/// Without this, `deltalake::open_table` returns `Cannot infer storage location from: s3://...`.
fn ensure_delta_handlers_registered() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        deltalake::aws::register_handlers(None);
    });
}

/// Strongly typed error domain for the Delta sink.
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

enum DeltaDestination {
    Local(PathBuf),
    S3 {
        prefix: String,
        client: Arc<dyn ObjectStore>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaFormat {
    Csv,
    Parquet,
    JsonL,
    Avro,
    Orc,
}

pub struct DeltaSinkOperator {
    table_name: String,
    destination: DeltaDestination,
    parquet_compression: Compression,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    sink_memory_block: Option<Arc<MemoryBlock>>,
    early_flush_threshold_bytes: usize,
    file_counter: u64,
    format: DeltaFormat,
    committer: Option<DeltaTableCommitter>,
    table_uri: Option<Url>,
    storage_options: HashMap<String, String>,
    commit_strategy: DeltaCommitStrategy,
    s3_bucket: Option<String>,
    sink_path: String,
    catalog_schema: Option<Arc<ArrowSchema>>,
    /// Normalized schema for Parquet files (microsecond timestamps, etc.).
    parquet_write_schema: Option<Arc<ArrowSchema>>,
}

impl DeltaSinkOperator {
    /// Synchronous, side-effect-free constructor. Async setup runs in [`Operator::on_start`].
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

        let destination = if let Some(bucket) = &s3_bucket {
            let region = options
                .get(opt::S3_REGION)
                .map(|s| s.as_str())
                .unwrap_or("us-east-1");
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket.clone())
                .with_region(region);

            if let Some(endpoint) = options.get(opt::S3_ENDPOINT) {
                builder = builder.with_endpoint(endpoint);
                if endpoint.to_ascii_lowercase().starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
            }
            if let Some(v) = options.get(opt::S3_ACCESS_KEY_ID) {
                builder = builder.with_access_key_id(v);
            }
            if let Some(v) = options.get(opt::S3_SECRET_ACCESS_KEY) {
                builder = builder.with_secret_access_key(v);
            }
            if let Some(v) = options.get(opt::S3_SESSION_TOKEN) {
                builder = builder.with_token(v);
            }

            let client = builder.build().map_err(DeltaSinkError::ObjectStore)?;

            DeltaDestination::S3 {
                prefix: path.trim_matches('/').to_string(),
                client: Arc::new(client),
            }
        } else {
            DeltaDestination::Local(PathBuf::from(path.clone()))
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
            destination,
            parquet_compression,
            pending: Vec::with_capacity(32),
            pending_bytes: 0,
            sink_memory_block,
            early_flush_threshold_bytes,
            file_counter: 0,
            format,
            committer: None,
            table_uri: None,
            storage_options,
            commit_strategy,
            s3_bucket,
            sink_path: path,
            catalog_schema: catalog_schema.and_then(strip_streaming_system_columns_arc),
            parquet_write_schema: None,
        })
    }

    /// Flush physical data files only; transaction commit is deferred to checkpoint.
    #[instrument(skip(self), fields(table = %self.table_name))]
    async fn flush_data_file(
        &mut self,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<(), DeltaSinkError> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let fallback_schema = self.catalog_schema.is_none().then(|| {
            strip_streaming_system_columns_arc(self.pending[0].schema())
                .expect("batch has no user columns after removing streaming system columns")
        });
        let batches = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;

        let record_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let format = self.format;
        let compression = self.parquet_compression;
        let parquet_write_schema = self.parquet_write_schema.clone();

        let bytes = tokio::task::spawn_blocking(move || {
            let batches = if format == DeltaFormat::Parquet {
                if let Some(ref schema) = parquet_write_schema {
                    cast_batches_for_delta_write(&batches, schema)?
                } else {
                    batches
                }
            } else {
                batches
            };
            match format {
                DeltaFormat::Csv => FormatEncoder::encode_csv(&batches),
                DeltaFormat::Parquet => FormatEncoder::encode_parquet(&batches, compression),
                DeltaFormat::JsonL => FormatEncoder::encode_jsonl(&batches),
                DeltaFormat::Avro => FormatEncoder::encode_avro(&batches),
                DeltaFormat::Orc => FormatEncoder::encode_orc(&batches),
            }
        })
        .await
        .map_err(|e| DeltaSinkError::SerializationPanic(e.to_string()))?
        .map_err(|e| DeltaSinkError::SerializationPanic(e.to_string()))?;

        if bytes.is_empty() {
            return Ok(());
        }

        self.file_counter += 1;
        let ext = match self.format {
            DeltaFormat::Csv => "csv",
            DeltaFormat::Parquet => "parquet",
            DeltaFormat::JsonL => "jsonl",
            DeltaFormat::Avro => "avro",
            DeltaFormat::Orc => "orc",
        };

        let file_name = format!(
            "part-{subtask_idx:05}-epoch-{epoch:010}-{counter:06}.{ext}",
            counter = self.file_counter
        );
        let file_size = bytes.len() as u64;
        let mut relative_path = file_name.clone();

        match &self.destination {
            DeltaDestination::Local(root) => {
                let out = root.join(&file_name);
                let mut f = tokio::fs::File::create(&out).await?;
                f.write_all(&bytes).await?;
                f.flush().await?;
            }
            DeltaDestination::S3 { prefix, client } => {
                let key = if prefix.is_empty() {
                    file_name
                } else {
                    format!("{prefix}/{file_name}")
                };
                relative_path = key.clone();
                client
                    .put(
                        &ObjectStorePath::from(key),
                        PutPayload::from(Bytes::from(bytes)),
                    )
                    .await?;
            }
        }

        if let Some(committer) = self.committer.as_mut() {
            if let Some(schema) = fallback_schema {
                committer.update_schema(schema)?;
            }
            committer.register_uncommitted(UncommittedDataFile {
                path: relative_path,
                size_bytes: file_size,
                record_count,
            });
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

        if let DeltaDestination::Local(root) = &self.destination {
            tokio::fs::create_dir_all(root).await?;
        }

        if let DeltaDestination::S3 { client, prefix } = &self.destination {
            preflight_s3_bucket(client.as_ref(), prefix, self.s3_bucket.as_deref()).await?;
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
            threshold = self.early_flush_threshold_bytes,
            is_true_delta = self.committer.is_some(),
            commit_strategy = %self.commit_strategy.label(),
            "delta sink operator started successfully"
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
        let batch_rows = batch.num_rows();
        let batch_bytes = batch.get_array_memory_size();
        self.pending_bytes += batch_bytes;
        self.pending.push(batch);

        debug!(
            table = %self.table_name,
            subtask_idx = ctx.subtask_index,
            batch_rows,
            batch_bytes,
            pending_batches = self.pending.len(),
            pending_bytes = self.pending_bytes,
            "delta sink received data"
        );

        if self.pending_bytes > self.early_flush_threshold_bytes {
            debug!(
                bytes = self.pending_bytes,
                "memory watermark reached, executing early flush (commit deferred to checkpoint)"
            );
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
            self.flush_and_commit_checkpoint(u64::MAX, ctx.subtask_index as usize)
                .await?;
        }
        Ok(vec![])
    }
}

/// Verify the S3 bucket is reachable and produce a clear error before
/// the deltalake layer fails with a less obvious message.
async fn preflight_s3_bucket(
    client: &dyn ObjectStore,
    prefix: &str,
    bucket: Option<&str>,
) -> Result<(), DeltaSinkError> {
    let probe_path = if prefix.is_empty() {
        ObjectStorePath::from("")
    } else {
        ObjectStorePath::from(prefix.to_string())
    };

    let mut stream = client.list(Some(&probe_path));
    match futures::StreamExt::next(&mut stream).await {
        None => Ok(()),
        Some(Ok(_)) => Ok(()),
        Some(Err(object_store::Error::NotFound { .. })) => Ok(()),
        Some(Err(e)) => {
            let msg = e.to_string();
            let lower = msg.to_ascii_lowercase();
            if lower.contains("nosuchbucket") || lower.contains("the specified bucket") {
                return Err(DeltaSinkError::Config(format!(
                    "S3 bucket '{}' does not exist (prefix '{}'): {msg}",
                    bucket.unwrap_or("<unknown>"),
                    prefix
                )));
            }
            Err(DeltaSinkError::Config(format!(
                "failed to access S3 bucket '{}' (prefix '{}'): {msg}",
                bucket.unwrap_or("<unknown>"),
                prefix
            )))
        }
    }
}
