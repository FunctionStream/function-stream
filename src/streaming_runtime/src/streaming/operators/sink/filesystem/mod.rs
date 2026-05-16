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

use std::fs::create_dir_all;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use parquet::basic::Compression;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::memory::{MemoryBlock, try_global_memory_pool};
use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::{CheckpointBarrier, Watermark};
use crate::streaming::StreamOutput;
use crate::streaming::api::context::TaskContext;
use crate::streaming::api::operator::{Collector, Operator};
use crate::streaming::format::encoder::FormatEncoder;

const DEFAULT_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub enum FilesystemFormat {
    Csv,
    Parquet,
    JsonL,
    Avro,
    Orc,
}

pub struct FilesystemSinkOperator {
    table_name: String,
    output_dir: PathBuf,
    format: FilesystemFormat,
    parquet_compression: Compression,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    sink_memory_block: Option<std::sync::Arc<MemoryBlock>>,
    early_flush_threshold_bytes: usize,
    file_counter: u64,
}

impl FilesystemSinkOperator {
    pub fn try_new(
        table_name: String,
        output_dir: String,
        format: FilesystemFormat,
        parquet_compression: Compression,
        sink_memory_bytes: u64,
    ) -> Result<Self> {
        let output_dir_path = PathBuf::from(&output_dir);
        create_dir_all(&output_dir_path).with_context(|| {
            format!(
                "failed to create filesystem sink directory {}",
                output_dir_path.display()
            )
        })?;

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
            output_dir: output_dir_path,
            format,
            parquet_compression,
            pending: Vec::new(),
            pending_bytes: 0,
            sink_memory_block,
            early_flush_threshold_bytes,
            file_counter: 0,
        })
    }

    fn extension(&self) -> &'static str {
        match self.format {
            FilesystemFormat::Csv => "csv",
            FilesystemFormat::Parquet => "parquet",
            FilesystemFormat::JsonL => "jsonl",
            FilesystemFormat::Avro => "avro",
            FilesystemFormat::Orc => "orc",
        }
    }

    async fn flush_file_epoch(&mut self, epoch: u64, subtask_idx: usize) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let batches = std::mem::take(&mut self.pending);
        let format = self.format;
        let compression = self.parquet_compression;
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            match format {
                FilesystemFormat::Csv => FormatEncoder::encode_csv(&batches),
                FilesystemFormat::Parquet => FormatEncoder::encode_parquet(&batches, compression),
                FilesystemFormat::JsonL => FormatEncoder::encode_jsonl(&batches),
                FilesystemFormat::Avro => FormatEncoder::encode_avro(&batches),
                FilesystemFormat::Orc => FormatEncoder::encode_orc(&batches),
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
            "part-{:05}-epoch-{:010}-{:06}.{}",
            subtask_idx,
            epoch,
            self.file_counter,
            self.extension()
        );
        let output_file = self.output_dir.join(file_name);
        let mut f = tokio::fs::File::create(&output_file)
            .await
            .with_context(|| format!("failed creating sink file {}", output_file.display()))?;
        f.write_all(&bytes)
            .await
            .with_context(|| format!("failed writing sink file {}", output_file.display()))?;
        self.pending_bytes = 0;
        Ok(())
    }
}

#[async_trait]
impl Operator for FilesystemSinkOperator {
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
            path = %self.output_dir.display(),
            format = ?self.format,
            reserved_block_bytes,
            early_flush_threshold_bytes = self.early_flush_threshold_bytes,
            "Starting filesystem sink operator"
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
                "memory watermark reached, triggering early flush to filesystem"
            );
            self.flush_file_epoch(0, ctx.subtask_index as usize).await?;
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
        self.flush_file_epoch(barrier.epoch, ctx.subtask_index as usize)
            .await
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        if !self.pending.is_empty() {
            warn!(
                table = %self.table_name,
                "flushing remaining filesystem sink batches on close"
            );
            self.flush_file_epoch(u64::MAX, ctx.subtask_index as usize)
                .await?;
        }
        Ok(vec![])
    }
}

pub fn compression_from_str(v: Option<&str>) -> Result<Compression> {
    match v.unwrap_or("zstd").to_ascii_lowercase().as_str() {
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "lz4" => Ok(Compression::LZ4),
        "lz4_raw" => Ok(Compression::LZ4_RAW),
        other => bail!("unsupported parquet compression '{other}'"),
    }
}
