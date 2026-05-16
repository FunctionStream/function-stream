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
use std::io::Cursor;

use anyhow::{Context, Result, bail};
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::{info, warn};

use crate::sql::common::constants::factory_operator_name;
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{CheckpointBarrier, Watermark};
use crate::streaming::StreamOutput;
use crate::streaming::api::context::TaskContext;
use crate::streaming::api::operator::{Collector, Operator};

#[derive(Debug, Clone, Copy)]
pub enum S3Format {
    Csv,
    Parquet,
}

pub struct S3SinkOperator {
    table_name: String,
    bucket: String,
    prefix: String,
    format: S3Format,
    parquet_compression: Compression,
    client: Box<dyn ObjectStore>,
    pending: Vec<RecordBatch>,
    file_counter: u64,
}

impl S3SinkOperator {
    pub fn try_new(
        table_name: String,
        path: String,
        format: S3Format,
        parquet_compression: Compression,
        s3_options: HashMap<String, String>,
    ) -> Result<Self> {
        let bucket = s3_options
            .get(opt::S3_BUCKET)
            .cloned()
            .context("s3 sink requires 's3.bucket'")?;
        let region = s3_options
            .get(opt::S3_REGION)
            .cloned()
            .unwrap_or_else(|| "us-east-1".to_string());

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket.clone())
            .with_region(region);
        if let Some(endpoint) = s3_options.get(opt::S3_ENDPOINT) {
            builder = builder.with_endpoint(endpoint);
            if endpoint.to_ascii_lowercase().starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(v) = s3_options.get(opt::S3_ACCESS_KEY_ID) {
            builder = builder.with_access_key_id(v);
        }
        if let Some(v) = s3_options.get(opt::S3_SECRET_ACCESS_KEY) {
            builder = builder.with_secret_access_key(v);
        }
        if let Some(v) = s3_options.get(opt::S3_SESSION_TOKEN) {
            builder = builder.with_token(v);
        }
        let client = builder
            .build()
            .context("failed to build s3 object-store client")?;

        let prefix = path.trim_matches('/').to_string();

        Ok(Self {
            table_name,
            bucket,
            prefix,
            format,
            parquet_compression,
            client: Box::new(client),
            pending: Vec::new(),
            file_counter: 0,
        })
    }

    fn extension(&self) -> &'static str {
        match self.format {
            S3Format::Csv => "csv",
            S3Format::Parquet => "parquet",
        }
    }

    fn serialize_csv(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut out);
        for batch in &self.pending {
            writer.write(batch).context("failed writing csv batch")?;
        }
        drop(writer);
        Ok(out)
    }

    fn serialize_parquet(&self) -> Result<Vec<u8>> {
        let schema = self
            .pending
            .first()
            .map(|b| b.schema())
            .context("parquet serialization requires at least one record batch")?;
        let props = WriterProperties::builder()
            .set_compression(self.parquet_compression)
            .build();
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))
            .context("failed to initialize parquet writer")?;
        for batch in &self.pending {
            writer
                .write(batch)
                .context("failed writing parquet batch")?;
        }
        writer.close().context("failed to close parquet writer")?;
        Ok(cursor.into_inner())
    }

    async fn flush_epoch(&mut self, epoch: u64, subtask_idx: usize, bytes: Vec<u8>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if bytes.is_empty() {
            self.pending.clear();
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
        let key = if self.prefix.is_empty() {
            file_name
        } else {
            format!("{}/{}", self.prefix, file_name)
        };
        self.client
            .put(
                &ObjectStorePath::from(key),
                PutPayload::from(Bytes::from(bytes)),
            )
            .await
            .context("failed writing object to s3")?;

        self.pending.clear();
        Ok(())
    }
}

#[async_trait]
impl Operator for S3SinkOperator {
    fn name(&self) -> &str {
        factory_operator_name::CONNECTOR_SINK
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        info!(
            table = %self.table_name,
            bucket = %self.bucket,
            prefix = %self.prefix,
            format = ?self.format,
            "Starting s3 sink operator"
        );
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
        _collector: &mut dyn Collector,
    ) -> Result<()> {
        self.pending.push(batch);
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
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = match self.format {
            S3Format::Csv => self.serialize_csv()?,
            S3Format::Parquet => self.serialize_parquet()?,
        };
        self.flush_epoch(barrier.epoch, ctx.subtask_index as usize, bytes)
            .await
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        if !self.pending.is_empty() {
            warn!(
                table = %self.table_name,
                "flushing remaining s3 sink batches on close"
            );
            let bytes = match self.format {
                S3Format::Csv => self.serialize_csv()?,
                S3Format::Parquet => self.serialize_parquet()?,
            };
            self.flush_epoch(0, ctx.subtask_index as usize, bytes)
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
