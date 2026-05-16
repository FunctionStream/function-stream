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

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, PutPayload};
use parquet::basic::Compression;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::memory::{MemoryBlock, try_global_memory_pool};
use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{CheckpointBarrier, Watermark};
use crate::streaming::StreamOutput;
use crate::streaming::api::context::TaskContext;
use crate::streaming::api::operator::{Collector, Operator};
use crate::streaming::format::encoder::FormatEncoder;

const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

enum IcebergDestination {
    Local(PathBuf),
    S3 {
        prefix: String,
        client: Arc<dyn ObjectStore>,
    },
}

pub struct IcebergSinkOperator {
    table_name: String,
    destination: IcebergDestination,
    parquet_compression: Compression,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    sink_memory_block: Option<Arc<MemoryBlock>>,
    early_flush_threshold_bytes: usize,
    file_counter: u64,
    format: IcebergFormat,
}

#[derive(Debug, Clone, Copy)]
pub enum IcebergFormat {
    Csv,
    Parquet,
}

impl IcebergSinkOperator {
    pub fn try_new(
        table_name: String,
        path: String,
        format: IcebergFormat,
        parquet_compression: Compression,
        sink_memory_bytes: u64,
        options: HashMap<String, String>,
    ) -> Result<Self> {
        let destination = if let Some(bucket) = options.get(opt::S3_BUCKET) {
            let region = options
                .get(opt::S3_REGION)
                .cloned()
                .unwrap_or_else(|| "us-east-1".to_string());
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
            let client = builder
                .build()
                .context("failed to build s3 client for iceberg sink")?;
            IcebergDestination::S3 {
                prefix: path.trim_matches('/').to_string(),
                client: Arc::new(client),
            }
        } else {
            let root = PathBuf::from(path.clone());
            create_dir_all(&root)
                .with_context(|| format!("failed to create iceberg sink dir {}", root.display()))?;
            IcebergDestination::Local(root)
        };

        let mut sink_memory_block = None;
        let reserve_bytes = usize::try_from(sink_memory_bytes).unwrap_or(DEFAULT_MAX_BUFFER_BYTES);
        let mut early_flush_threshold_bytes = reserve_bytes;
        if let Ok(pool) = try_global_memory_pool()
            && let Ok(block) = pool.try_request_block(reserve_bytes as u64)
        {
            early_flush_threshold_bytes = ((block.capacity() as usize) * 8) / 10;
            sink_memory_block = Some(block);
        }

        Ok(Self {
            table_name,
            destination,
            parquet_compression,
            pending: Vec::new(),
            pending_bytes: 0,
            sink_memory_block,
            early_flush_threshold_bytes,
            file_counter: 0,
            format,
        })
    }

    async fn flush_epoch(&mut self, epoch: u64, subtask_idx: usize) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let batches = std::mem::take(&mut self.pending);
        let format = self.format;
        let compression = self.parquet_compression;
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            match format {
                IcebergFormat::Csv => FormatEncoder::encode_csv(&batches),
                IcebergFormat::Parquet => FormatEncoder::encode_parquet(&batches, compression),
            }
        })
        .await
        .context("tokio blocking task panicked during serialization")??;

        if bytes.is_empty() {
            self.pending_bytes = 0;
            return Ok(());
        }

        self.file_counter += 1;
        let file_name = format!(
            "iceberg-part-{:05}-epoch-{:010}-{:06}.{}",
            subtask_idx,
            epoch,
            self.file_counter,
            match self.format {
                IcebergFormat::Csv => "csv",
                IcebergFormat::Parquet => "parquet",
            }
        );
        match &self.destination {
            IcebergDestination::Local(root) => {
                let out = root.join(file_name);
                let mut f = tokio::fs::File::create(&out).await.with_context(|| {
                    format!("failed creating iceberg sink file {}", out.display())
                })?;
                f.write_all(&bytes).await.with_context(|| {
                    format!("failed writing iceberg sink file {}", out.display())
                })?;
            }
            IcebergDestination::S3 { prefix, client } => {
                let key = if prefix.is_empty() {
                    file_name
                } else {
                    format!("{prefix}/{file_name}")
                };
                client
                    .put(
                        &ObjectStorePath::from(key),
                        PutPayload::from(Bytes::from(bytes)),
                    )
                    .await
                    .context("failed writing iceberg data object to s3")?;
            }
        }
        self.pending_bytes = 0;
        Ok(())
    }
}

#[async_trait]
impl Operator for IcebergSinkOperator {
    fn name(&self) -> &str {
        factory_operator_name::CONNECTOR_SINK
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        let reserved_block_bytes = self
            .sink_memory_block
            .as_ref()
            .map(|b| b.capacity())
            .unwrap_or(0);
        info!(
            table = %self.table_name,
            format = ?self.format,
            reserved_block_bytes,
            early_flush_threshold_bytes = self.early_flush_threshold_bytes,
            "Starting iceberg sink operator"
        );
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        self.pending.push(batch);
        self.pending_bytes += batch_size;
        if self.pending_bytes > self.early_flush_threshold_bytes {
            debug!(
                table = %self.table_name,
                bytes = self.pending_bytes,
                threshold = self.early_flush_threshold_bytes,
                "memory watermark reached, triggering early flush for iceberg sink"
            );
            self.flush_epoch(0, ctx.subtask_index as usize).await?;
        }
        Ok(())
    }

    async fn process_watermark(
        &mut self,
        _watermark: Watermark,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        Ok(())
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> Result<()> {
        self.flush_epoch(barrier.epoch, ctx.subtask_index as usize)
            .await
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        if !self.pending.is_empty() {
            warn!(
                table = %self.table_name,
                "flushing remaining iceberg sink batches on close"
            );
            self.flush_epoch(u64::MAX, ctx.subtask_index as usize)
                .await?;
        }
        Ok(vec![])
    }
}
