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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Add, StructField, StructType};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, open_table_with_storage_options};
use tracing::{info, instrument};
use url::Url;

use super::DeltaSinkError;

pub struct UncommittedDataFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

pub struct DeltaTableCommitter {
    table_uri: Url,
    storage_options: HashMap<String, String>,
    uncommitted: Vec<UncommittedDataFile>,
    table: Option<DeltaTable>,
    /// Precomputed from catalog `fs_schema` at startup, or lazily from the first batch.
    delta_columns: Option<Vec<StructField>>,
}

impl DeltaTableCommitter {
    pub fn try_new(
        table_uri: Url,
        storage_options: HashMap<String, String>,
        catalog_schema: Option<Arc<ArrowSchema>>,
    ) -> Result<Self, DeltaSinkError> {
        let delta_columns = catalog_schema
            .as_deref()
            .map(arrow_schema_to_delta_columns)
            .transpose()?;

        Ok(Self {
            table_uri,
            storage_options,
            uncommitted: Vec::new(),
            table: None,
            delta_columns,
        })
    }

    /// Fallback when catalog schema is absent: derive columns from the first flushed batch.
    pub fn update_schema(&mut self, schema: Arc<ArrowSchema>) -> Result<(), DeltaSinkError> {
        if self.delta_columns.is_none() {
            self.delta_columns = Some(arrow_schema_to_delta_columns(&schema)?);
        }
        Ok(())
    }

    pub fn register_uncommitted(&mut self, file: UncommittedDataFile) {
        self.uncommitted.push(file);
    }

    pub fn has_uncommitted(&self) -> bool {
        !self.uncommitted.is_empty()
    }

    async fn ensure_table(&mut self) -> Result<(), DeltaSinkError> {
        if self.table.is_some() {
            return Ok(());
        }

        let open_result = if self.storage_options.is_empty() {
            deltalake::open_table(self.table_uri.clone()).await
        } else {
            open_table_with_storage_options(self.table_uri.clone(), self.storage_options.clone())
                .await
        };

        match open_result {
            Ok(table) => {
                let version = table.version();
                info!(
                    table_uri = %self.table_uri,
                    ?version,
                    "opened existing delta table"
                );
                self.table = Some(table);
                return Ok(());
            }
            Err(DeltaTableError::NotATable(_)) => {}
            Err(e) => {
                return Err(DeltaSinkError::CommitterFailed(e.to_string()));
            }
        }

        let columns = self.delta_columns.as_ref().ok_or_else(|| {
            DeltaSinkError::Config(
                "no catalog schema and no batch schema available before commit".into(),
            )
        })?;

        let table = if self.storage_options.is_empty() {
            DeltaTable::try_from_url(self.table_uri.clone())
                .await
                .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?
        } else {
            DeltaTable::try_from_url_with_storage_options(
                self.table_uri.clone(),
                self.storage_options.clone(),
            )
            .await
            .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?
        };

        let table = table
            .create()
            .with_columns(columns.clone())
            .await
            .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;

        info!(
            table_uri = %self.table_uri,
            column_count = columns.len(),
            version = table.version(),
            "created new delta table"
        );
        self.table = Some(table);
        Ok(())
    }

    #[instrument(skip(self), fields(table_uri = %self.table_uri, epoch))]
    pub async fn commit_checkpoint(&mut self, epoch: u64) -> Result<(), DeltaSinkError> {
        if self.uncommitted.is_empty() {
            return Ok(());
        }

        let files = std::mem::take(&mut self.uncommitted);
        let file_count = files.len();
        let total_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
        let total_records: u64 = files.iter().map(|f| f.record_count).sum();
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();

        let prev_version = self.table.as_ref().and_then(|t| t.version());

        info!(
            file_count,
            total_bytes,
            total_records,
            ?prev_version,
            paths = ?paths,
            "committing delta checkpoint"
        );

        self.ensure_table().await?;

        let table = self
            .table
            .as_mut()
            .ok_or_else(|| DeltaSinkError::CommitterFailed("delta table not loaded".into()))?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let adds: Vec<Action> = files
            .iter()
            .map(|f| {
                Action::Add(Add {
                    path: f.path.clone(),
                    size: f.size_bytes as i64,
                    modification_time: now_ms,
                    data_change: true,
                    partition_values: HashMap::new(),
                    stats: simple_stats_json(f.record_count),
                    ..Default::default()
                })
            })
            .collect();

        let snapshot = table
            .snapshot()
            .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let finalized = CommitBuilder::default()
            .with_actions(adds)
            .build(Some(snapshot), table.log_store().clone(), operation)
            .await
            .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;

        let version = finalized.version();
        table.state = Some(finalized.snapshot());
        table
            .load()
            .await
            .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;

        info!(
            epoch,
            version,
            file_count,
            total_bytes,
            total_records,
            ?prev_version,
            paths = ?paths,
            "delta checkpoint committed"
        );
        Ok(())
    }
}

/// Bridge arrow 55 (runtime) schema to deltalake kernel schema via IPC.
fn arrow_schema_to_delta_columns(schema: &ArrowSchema) -> Result<Vec<StructField>, DeltaSinkError> {
    let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &empty.schema()).map_err(|e| {
            DeltaSinkError::CommitterFailed(format!("ipc schema encode failed: {e}"))
        })?;
        writer.write(&empty).map_err(|e| {
            DeltaSinkError::CommitterFailed(format!("ipc schema encode failed: {e}"))
        })?;
        writer.finish().map_err(|e| {
            DeltaSinkError::CommitterFailed(format!("ipc schema encode failed: {e}"))
        })?;
    }

    let cursor = std::io::Cursor::new(buf);
    let reader = deltalake::arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| DeltaSinkError::CommitterFailed(format!("ipc schema decode failed: {e}")))?;
    let delta_schema = reader.schema();
    let struct_type: StructType = delta_schema
        .try_into_kernel()
        .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;
    Ok(struct_type.fields().cloned().collect())
}

fn simple_stats_json(record_count: u64) -> Option<String> {
    Some(format!(r#"{{"numRecords":{record_count}}}"#))
}

pub fn build_delta_storage_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    use crate::sql::common::with_option_keys as opt;

    let mut storage = HashMap::new();
    if let Some(v) = options.get(opt::S3_ACCESS_KEY_ID) {
        storage.insert("AWS_ACCESS_KEY_ID".to_string(), v.clone());
    }
    if let Some(v) = options.get(opt::S3_SECRET_ACCESS_KEY) {
        storage.insert("AWS_SECRET_ACCESS_KEY".to_string(), v.clone());
    }
    if let Some(v) = options.get(opt::S3_REGION) {
        storage.insert("AWS_REGION".to_string(), v.clone());
    }
    if let Some(v) = options.get(opt::S3_ENDPOINT) {
        storage.insert("AWS_ENDPOINT_URL".to_string(), v.clone());
        if !v.starts_with("https://") {
            storage.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        }
    }
    if let Some(v) = options.get(opt::S3_SESSION_TOKEN) {
        storage.insert("AWS_SESSION_TOKEN".to_string(), v.clone());
    }
    storage
}

pub fn resolve_delta_table_uri(path: &str, bucket: Option<&str>) -> Result<Url, DeltaSinkError> {
    if path.contains("://") {
        return Url::parse(path).map_err(|e| DeltaSinkError::Config(e.to_string()));
    }
    if let Some(bucket) = bucket {
        let trimmed = path.trim_matches('/');
        let uri = if trimmed.is_empty() {
            format!("s3://{bucket}")
        } else {
            format!("s3://{bucket}/{trimmed}")
        };
        return Url::parse(&uri).map_err(|e| DeltaSinkError::Config(e.to_string()));
    }
    let canonical = std::fs::canonicalize(path).map_err(DeltaSinkError::Io)?;
    Url::from_directory_path(canonical)
        .map_err(|_| DeltaSinkError::Config(format!("invalid local delta table path: {path}")))
}
