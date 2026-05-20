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

use arrow::compute::cast;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema, TimeUnit};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
use deltalake::kernel::schema::cast::normalize_for_delta;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Add, StructField, StructType};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, open_table_with_storage_options};
use tracing::{info, instrument};
use url::Url;

use crate::sql::common::{TIMESTAMP_FIELD, UPDATING_META_FIELD};

use super::DeltaSinkError;

/// Coordination strategy for Delta `_delta_log` commits on S3-compatible stores.
///
/// `deltalake-aws` 0.15 only ships two safe primitives for S3 commits:
///   - a DynamoDB-backed lock client, or
///   - a single-writer flag (`AWS_S3_ALLOW_UNSAFE_RENAME=true`) that
///     guarantees correctness only when there is exactly one writer per table.
///
/// Function-Stream's planner+scheduler guarantees a single writer per Delta
/// table, so `SingleWriter` is the safe default for all S3-compatible backends
/// including MinIO, R2 and GCS-S3. For multi-process deployments writing to the
/// same table, switch to `DynamoDb`.
#[derive(Debug, Clone)]
pub enum DeltaCommitStrategy {
    /// Single-writer per table (default). Function-Stream's scheduler enforces
    /// this invariant, so direct PUTs to `_delta_log/*.json` are race-free.
    SingleWriter,

    /// DynamoDB-backed multi-writer concurrency control via deltalake-aws.
    DynamoDb(DynamoDbLockConfig),
}

#[derive(Debug, Clone, Default)]
pub struct DynamoDbLockConfig {
    pub table_name: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl DeltaCommitStrategy {
    /// Resolve the commit strategy from sink options.
    ///
    /// `delta.commit.strategy`:
    ///   - `single-writer` / `single_writer` / `none` / unset → SingleWriter
    ///   - `dynamodb` / `dynamo` → DynamoDb
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, DeltaSinkError> {
        use crate::sql::common::with_option_keys as opt;

        let raw = options
            .get(opt::DELTA_COMMIT_STRATEGY)
            .map(|v| v.trim().to_ascii_lowercase())
            .unwrap_or_default();

        match raw.as_str() {
            "" | "single-writer" | "single_writer" | "singlewriter" | "none" => {
                Ok(Self::SingleWriter)
            }
            "dynamodb" | "dynamo" => {
                let cfg = DynamoDbLockConfig {
                    table_name: options.get(opt::DELTA_DYNAMODB_TABLE).cloned(),
                    region: options.get(opt::DELTA_DYNAMODB_REGION).cloned(),
                    endpoint: options.get(opt::DELTA_DYNAMODB_ENDPOINT).cloned(),
                    access_key_id: options.get(opt::DELTA_DYNAMODB_ACCESS_KEY_ID).cloned(),
                    secret_access_key: options.get(opt::DELTA_DYNAMODB_SECRET_ACCESS_KEY).cloned(),
                };
                if cfg.table_name.is_none() {
                    return Err(DeltaSinkError::Config(
                        "delta.commit.strategy=dynamodb requires delta.dynamodb.table".into(),
                    ));
                }
                Ok(Self::DynamoDb(cfg))
            }
            other => Err(DeltaSinkError::Config(format!(
                "unknown delta.commit.strategy '{other}'; expected single-writer or dynamodb"
            ))),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::SingleWriter => "single-writer",
            Self::DynamoDb(_) => "dynamodb",
        }
    }
}

/// Streaming-internal columns that must not be persisted to external sinks.
pub fn is_streaming_system_column(name: &str) -> bool {
    name == TIMESTAMP_FIELD || name == UPDATING_META_FIELD
}

/// Remove `_timestamp` / `_updating_meta` from a schema (e.g. connector `fs_schema` may inject them).
pub fn strip_streaming_system_columns(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .filter(|f| !is_streaming_system_column(f.name()))
        .cloned()
        .collect();
    ArrowSchema::new(fields)
}

pub fn strip_streaming_system_columns_arc(schema: Arc<ArrowSchema>) -> Option<Arc<ArrowSchema>> {
    let stripped = strip_streaming_system_columns(schema.as_ref());
    if stripped.fields().is_empty() {
        return None;
    }
    let had_system = schema
        .fields()
        .iter()
        .any(|f| is_streaming_system_column(f.name()));
    if had_system {
        Some(Arc::new(stripped))
    } else {
        Some(schema)
    }
}

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
    /// Arrow 55 schema for Parquet writes (timestamps normalized to microsecond).
    write_schema: Option<Arc<ArrowSchema>>,
}

impl DeltaTableCommitter {
    pub fn try_new(
        table_uri: Url,
        storage_options: HashMap<String, String>,
        catalog_schema: Option<Arc<ArrowSchema>>,
    ) -> Result<Self, DeltaSinkError> {
        let user_schema = catalog_schema.and_then(strip_streaming_system_columns_arc);
        let write_schema = user_schema
            .as_ref()
            .map(|s| Arc::new(normalize_arrow_schema_for_delta(s)));
        let delta_columns = user_schema
            .as_deref()
            .map(arrow_schema_to_delta_columns)
            .transpose()?;

        Ok(Self {
            table_uri,
            storage_options,
            uncommitted: Vec::new(),
            table: None,
            delta_columns,
            write_schema,
        })
    }

    pub fn write_schema(&self) -> Option<Arc<ArrowSchema>> {
        self.write_schema.clone()
    }

    /// Fallback when catalog schema is absent: derive user columns from the first flushed batch.
    pub fn update_schema(&mut self, schema: Arc<ArrowSchema>) -> Result<(), DeltaSinkError> {
        let user_schema = strip_streaming_system_columns_arc(schema).ok_or_else(|| {
            DeltaSinkError::CommitterFailed(
                "cannot derive delta table schema: no user columns after removing streaming \
                 system columns (_timestamp, _updating_meta)"
                    .into(),
            )
        })?;
        if self.delta_columns.is_none() {
            self.delta_columns = Some(arrow_schema_to_delta_columns(user_schema.as_ref())?);
        }
        if self.write_schema.is_none() {
            self.write_schema = Some(Arc::new(normalize_arrow_schema_for_delta(
                user_schema.as_ref(),
            )));
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

/// Normalize Arrow 55 schema for Delta Parquet writes (align with deltalake `normalize_for_delta`).
pub fn normalize_arrow_schema_for_delta(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| Arc::new(normalize_field_for_delta(f.as_ref())))
        .collect();
    ArrowSchema::new(fields)
}

fn normalize_field_for_delta(field: &Field) -> Field {
    let data_type = normalize_datatype_for_delta(field.data_type());
    if data_type == *field.data_type() {
        field.clone()
    } else {
        field.clone().with_data_type(data_type)
    }
}

fn normalize_datatype_for_delta(dt: &DataType) -> DataType {
    match dt {
        DataType::Date64 => DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, tz)
        | DataType::Timestamp(TimeUnit::Millisecond, tz)
        | DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
        }
        DataType::Struct(fields) => {
            let normalized: Vec<FieldRef> = fields
                .iter()
                .map(|f| Arc::new(normalize_field_for_delta(f.as_ref())))
                .collect();
            DataType::Struct(normalized.into())
        }
        DataType::List(inner) => {
            DataType::List(Arc::new(normalize_field_for_delta(inner.as_ref())))
        }
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(normalize_field_for_delta(inner.as_ref())))
        }
        DataType::FixedSizeList(inner, len) => {
            DataType::FixedSizeList(Arc::new(normalize_field_for_delta(inner.as_ref())), *len)
        }
        DataType::Map(entries, sorted) => DataType::Map(
            Arc::new(normalize_field_for_delta(entries.as_ref())),
            *sorted,
        ),
        _ => dt.clone(),
    }
}

/// Cast record batches so on-disk Parquet matches the Delta table schema.
pub fn cast_batches_for_delta_write(
    batches: &[RecordBatch],
    target_schema: &ArrowSchema,
) -> Result<Vec<RecordBatch>, DeltaSinkError> {
    let target = Arc::new(target_schema.clone());
    batches
        .iter()
        .map(|batch| cast_batch_for_delta_write(batch, &target))
        .collect()
}

fn cast_batch_for_delta_write(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch, DeltaSinkError> {
    if batch.schema().as_ref() == target_schema.as_ref() {
        return Ok(batch.clone());
    }

    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let col = batch.column_by_name(field.name()).ok_or_else(|| {
            DeltaSinkError::CommitterFailed(format!(
                "batch missing column '{}' required by delta write schema",
                field.name()
            ))
        })?;
        let casted = if col.data_type() == field.data_type() {
            col.clone()
        } else {
            cast(col, field.data_type()).map_err(|e| {
                DeltaSinkError::CommitterFailed(format!(
                    "failed to cast column '{}' from {:?} to {:?}: {e}",
                    field.name(),
                    col.data_type(),
                    field.data_type()
                ))
            })?
        };
        columns.push(casted);
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DeltaSinkError::CommitterFailed(format!("failed to build delta write batch: {e}"))
    })
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
    // Delta kernel rejects Timestamp(ms); normalize to Delta-compatible types (e.g. us).
    let normalized = normalize_for_delta(&delta_schema);
    let struct_type: StructType = normalized
        .as_ref()
        .try_into_kernel()
        .map_err(|e| DeltaSinkError::CommitterFailed(e.to_string()))?;
    Ok(struct_type.fields().cloned().collect())
}

fn simple_stats_json(record_count: u64) -> Option<String> {
    Some(format!(r#"{{"numRecords":{record_count}}}"#))
}

/// Build the storage options handed to deltalake.
///
/// Returns both the assembled HashMap and the resolved commit strategy so the
/// caller can log/expose it.
pub fn build_delta_storage_options(
    options: &HashMap<String, String>,
) -> Result<(HashMap<String, String>, DeltaCommitStrategy), DeltaSinkError> {
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
        // Custom endpoint implies MinIO / Ceph / R2 / private S3 gateway — these
        // backends route via path style; virtual-hosted style requires per-bucket
        // DNS that they typically do not provide.
        storage.insert(
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST".to_string(),
            "false".to_string(),
        );
    }
    if let Some(v) = options.get(opt::S3_SESSION_TOKEN) {
        storage.insert("AWS_SESSION_TOKEN".to_string(), v.clone());
    }

    let strategy = DeltaCommitStrategy::from_options(options)?;
    match &strategy {
        DeltaCommitStrategy::SingleWriter => {
            // Function-Stream guarantees a single writer per Delta table via its
            // pipeline scheduler. deltalake-aws still gates S3 commits unless one
            // of the two supported flags is set, so we set the single-writer flag
            // here; the architectural invariant makes this safe.
            storage.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
        }
        DeltaCommitStrategy::DynamoDb(cfg) => {
            storage.insert(
                "AWS_S3_LOCKING_PROVIDER".to_string(),
                "dynamodb".to_string(),
            );
            if let Some(table) = &cfg.table_name {
                storage.insert("DELTA_DYNAMO_TABLE_NAME".to_string(), table.clone());
            }
            if let Some(region) = &cfg.region {
                storage.insert("AWS_REGION_DYNAMODB".to_string(), region.clone());
            }
            if let Some(endpoint) = &cfg.endpoint {
                storage.insert("AWS_ENDPOINT_URL_DYNAMODB".to_string(), endpoint.clone());
            }
            if let Some(v) = &cfg.access_key_id {
                storage.insert("AWS_ACCESS_KEY_ID_DYNAMODB".to_string(), v.clone());
            }
            if let Some(v) = &cfg.secret_access_key {
                storage.insert("AWS_SECRET_ACCESS_KEY_DYNAMODB".to_string(), v.clone());
            }
        }
    }

    Ok((storage, strategy))
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
