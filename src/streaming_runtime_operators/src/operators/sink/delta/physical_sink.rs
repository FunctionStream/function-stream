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

//! Arroyo-style physical file sink: encode + PUT to object storage without touching
//! `_delta_log`. The coordinator ([`super::DeltaSinkOperator`] + [`super::delta_commit::DeltaTableCommitter`])
//! commits metadata at checkpoint boundaries.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{BackoffConfig, ObjectStore, PutPayload, RetryConfig};
use parquet::basic::Compression;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use crate::format::encoder::FormatEncoder;
use crate::sql::common::with_option_keys as opt;

use super::delta_commit::cast_batches_for_delta_write;
use super::{DeltaFormat, DeltaSinkError};

/// Descriptor returned after a successful physical write (Arroyo `FinishedFile` contract).
/// Stateless and safe to hand to the transaction committer at checkpoint time.
#[derive(Debug, Clone)]
pub struct FinishedFile {
    pub filename: String,
    pub size: usize,
    pub record_count: u64,
}

impl FinishedFile {
    pub fn into_uncommitted(self) -> super::delta_commit::UncommittedDataFile {
        super::delta_commit::UncommittedDataFile {
            path: self.filename,
            size_bytes: self.size as u64,
            record_count: self.record_count,
        }
    }
}

/// Single-writer physical sink targeting S3-compatible stores (AWS S3, MinIO, R2, Ceph).
///
/// Uses plain `ObjectStore::put` for unique `part-*.parquet` keys — no `_delta_log` I/O and
/// no rename-based locking on this path. Delta transaction logs are written only by
/// [`super::delta_commit::DeltaTableCommitter`] at checkpoint.
pub struct SingleThreadPhysicalSink {
    bucket_name: String,
    prefix_path: String,
    object_client: Arc<dyn ObjectStore>,
    file_counter: u64,
}

impl SingleThreadPhysicalSink {
    pub fn try_new(
        bucket: String,
        prefix: String,
        options: &HashMap<String, String>,
    ) -> Result<Self, DeltaSinkError> {
        let client = build_s3_object_store(bucket.as_str(), options)?;
        Ok(Self {
            bucket_name: bucket,
            prefix_path: prefix.trim_matches('/').to_string(),
            object_client: client,
            file_counter: 0,
        })
    }

    pub fn client(&self) -> Arc<dyn ObjectStore> {
        self.object_client.clone()
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn prefix_path(&self) -> &str {
        &self.prefix_path
    }

    /// Encode batches off the async runtime, then atomically PUT one object.
    pub async fn write_batches(
        &mut self,
        batches: &[RecordBatch],
        format: DeltaFormat,
        compression: Compression,
        parquet_write_schema: Option<Arc<ArrowSchema>>,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<Option<FinishedFile>, DeltaSinkError> {
        if batches.is_empty() {
            return Ok(None);
        }

        let record_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let owned_batches = batches.to_vec();

        let encoded = tokio::task::spawn_blocking(move || {
            encode_batches(
                owned_batches,
                format,
                compression,
                parquet_write_schema.as_deref(),
            )
        })
        .await
        .map_err(|e| DeltaSinkError::SerializationPanic(e.to_string()))?
        .map_err(DeltaSinkError::SerializationPanic)?;

        self.put_encoded(encoded, record_count, format, epoch, subtask_idx)
            .await
    }

    async fn put_encoded(
        &mut self,
        encoded: Vec<u8>,
        record_count: u64,
        format: DeltaFormat,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<Option<FinishedFile>, DeltaSinkError> {
        if encoded.is_empty() {
            return Ok(None);
        }

        self.file_counter += 1;
        let ext = format.file_extension();
        let file_name = format!(
            "part-{subtask_idx:05}-epoch-{epoch:010}-{counter:06}.{ext}",
            counter = self.file_counter
        );
        let object_key = if self.prefix_path.is_empty() {
            file_name
        } else {
            format!("{}/{}", self.prefix_path, file_name)
        };

        let size = encoded.len();
        debug!(
            target_key = %object_key,
            size,
            record_count,
            "uploading data file to S3-compatible storage"
        );

        self.object_client
            .put(
                &ObjectStorePath::from(object_key.clone()),
                PutPayload::from(Bytes::from(encoded)),
            )
            .await?;

        info!(file = %object_key, size, record_count, "data file persisted to object storage");

        Ok(Some(FinishedFile {
            filename: object_key,
            size,
            record_count,
        }))
    }
}

/// Local filesystem physical sink (same contract as [`SingleThreadPhysicalSink`]).
pub struct LocalPhysicalSink {
    root: PathBuf,
    file_counter: u64,
}

impl LocalPhysicalSink {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            file_counter: 0,
        }
    }

    pub async fn write_batches(
        &mut self,
        batches: &[RecordBatch],
        format: DeltaFormat,
        compression: Compression,
        parquet_write_schema: Option<Arc<ArrowSchema>>,
        epoch: u64,
        subtask_idx: usize,
    ) -> Result<Option<FinishedFile>, DeltaSinkError> {
        if batches.is_empty() {
            return Ok(None);
        }

        let record_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let owned_batches = batches.to_vec();

        let encoded = tokio::task::spawn_blocking(move || {
            encode_batches(
                owned_batches,
                format,
                compression,
                parquet_write_schema.as_deref(),
            )
        })
        .await
        .map_err(|e| DeltaSinkError::SerializationPanic(e.to_string()))?
        .map_err(DeltaSinkError::SerializationPanic)?;

        if encoded.is_empty() {
            return Ok(None);
        }

        self.file_counter += 1;
        let ext = format.file_extension();
        let file_name = format!(
            "part-{subtask_idx:05}-epoch-{epoch:010}-{counter:06}.{ext}",
            counter = self.file_counter
        );
        let out = self.root.join(&file_name);
        let size = encoded.len();

        let mut f = tokio::fs::File::create(&out).await?;
        f.write_all(&encoded).await?;
        f.flush().await?;

        info!(file = %file_name, size, record_count, "data file persisted to local storage");

        Ok(Some(FinishedFile {
            filename: file_name,
            size,
            record_count,
        }))
    }
}

/// Build an S3-compatible [`ObjectStore`] client with production defaults for MinIO / private cloud.
pub fn build_s3_object_store(
    bucket: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, DeltaSinkError> {
    let region = options
        .get(opt::S3_REGION)
        .map(|s| s.as_str())
        .unwrap_or("us-east-1");

    let retry_config = RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(3),
            base: 2.0,
        },
        max_retries: 4,
        retry_timeout: Duration::from_secs(15),
    };

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_retry(retry_config);

    if let Some(endpoint) = options.get(opt::S3_ENDPOINT) {
        builder = builder.with_endpoint(endpoint);
        if endpoint.to_ascii_lowercase().starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_virtual_hosted_style_request(false);
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

    Ok(Arc::new(
        builder.build().map_err(DeltaSinkError::ObjectStore)?,
    ))
}

/// Preflight bucket connectivity before the pipeline accepts traffic.
pub async fn preflight_s3_bucket(
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
        None | Some(Ok(_)) => Ok(()),
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
            if lower.contains("invalidaccesskeyid")
                || lower.contains("signaturedoesnotmatch")
                || lower.contains("access denied")
                || lower.contains("accessdenied")
            {
                return Err(DeltaSinkError::Config(format!(
                    "S3 credential check failed for bucket '{}': {msg}",
                    bucket.unwrap_or("<unknown>")
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

fn encode_batches(
    batches: Vec<RecordBatch>,
    format: DeltaFormat,
    compression: Compression,
    parquet_write_schema: Option<&ArrowSchema>,
) -> Result<Vec<u8>, String> {
    let batches = if format == DeltaFormat::Parquet {
        if let Some(schema) = parquet_write_schema {
            cast_batches_for_delta_write(&batches, schema).map_err(|e| e.to_string())?
        } else {
            batches
        }
    } else {
        batches
    };

    match format {
        DeltaFormat::Csv => FormatEncoder::encode_csv(&batches).map_err(|e| e.to_string()),
        DeltaFormat::Parquet => {
            FormatEncoder::encode_parquet(&batches, compression).map_err(|e| e.to_string())
        }
        DeltaFormat::JsonL => FormatEncoder::encode_jsonl(&batches).map_err(|e| e.to_string()),
        DeltaFormat::Avro => FormatEncoder::encode_avro(&batches).map_err(|e| e.to_string()),
        DeltaFormat::Orc => FormatEncoder::encode_orc(&batches).map_err(|e| e.to_string()),
    }
}

impl DeltaFormat {
    fn file_extension(self) -> &'static str {
        match self {
            DeltaFormat::Csv => "csv",
            DeltaFormat::Parquet => "parquet",
            DeltaFormat::JsonL => "jsonl",
            DeltaFormat::Avro => "avro",
            DeltaFormat::Orc => "orc",
        }
    }
}
