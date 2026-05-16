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

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, bail};
use datafusion::common::{Result as DFResult, internal_err, plan_err};
use prost::Message;
use protocol::function_stream_graph::FsProgram;
use protocol::storage::{self as pb, table_definition};
use tracing::{debug, info, warn};

use unicase::UniCase;

use crate::sql::common::constants::sql_field;
use crate::sql::connector::config::ConnectorConfig;
use crate::sql::schema::catalog::{ExternalTable, LookupTable, SourceTable};
use crate::sql::schema::column_descriptor::ColumnDescriptor;
use crate::sql::schema::table::CatalogEntity;
use crate::sql::schema::table_role::TableRole;
use crate::sql::schema::temporal_pipeline_config::TemporalPipelineConfig;
use crate::sql::schema::{StreamPlanningContext, StreamTable};

use super::codec::CatalogCodec;
use super::meta_store::MetaStore;

const CATALOG_KEY_PREFIX: &str = "catalog:stream_table:";
const STREAMING_JOB_KEY_PREFIX: &str = "streaming_job:";

/// One persisted streaming job row from catalog (program + checkpoint metadata).
#[derive(Debug, Clone)]
pub struct StoredStreamingJob {
    pub table_name: String,
    pub program: FsProgram,
    pub checkpoint_interval_ms: u64,
    pub latest_checkpoint_epoch: u64,
    /// Source-type-agnostic per-subtask checkpoint entries. Each entry is a
    /// [`pb::SourceCheckpointInfo`] oneof envelope — the catalog does not inspect the payload.
    pub source_checkpoints: Vec<pb::SourceCheckpointInfo>,
}

pub struct CatalogManager {
    store: Arc<dyn MetaStore>,
}

static GLOBAL_CATALOG: OnceLock<Arc<CatalogManager>> = OnceLock::new();

impl CatalogManager {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }

    pub fn init_global_in_memory() -> anyhow::Result<()> {
        Self::init_global(Arc::new(super::InMemoryMetaStore::new()))
    }

    pub fn init_global(store: Arc<dyn MetaStore>) -> anyhow::Result<()> {
        if GLOBAL_CATALOG.get().is_some() {
            bail!("CatalogManager already initialized");
        }

        let mgr = Arc::new(CatalogManager::new(store));
        GLOBAL_CATALOG
            .set(mgr)
            .map_err(|_| anyhow!("CatalogManager global install failed"))?;

        Ok(())
    }

    pub fn try_global() -> Option<Arc<CatalogManager>> {
        GLOBAL_CATALOG.get().cloned()
    }

    pub fn global() -> anyhow::Result<Arc<CatalogManager>> {
        Self::try_global().ok_or_else(|| anyhow!("CatalogManager not initialized"))
    }

    #[inline]
    fn build_store_key(table_name: &str) -> String {
        format!("{CATALOG_KEY_PREFIX}{}", table_name.to_lowercase())
    }

    #[inline]
    fn build_streaming_job_key(table_name: &str) -> String {
        format!("{STREAMING_JOB_KEY_PREFIX}{}", table_name.to_lowercase())
    }

    // ========================================================================
    // Streaming job persistence (CREATE STREAMING TABLE / DROP STREAMING TABLE)
    // ========================================================================

    pub fn persist_streaming_job(
        &self,
        table_name: &str,
        fs_program: &FsProgram,
        comment: &str,
        checkpoint_interval_ms: u64,
    ) -> DFResult<()> {
        let program_bytes = fs_program.encode_to_vec();
        let def = pb::StreamingTableDefinition {
            table_name: table_name.to_string(),
            created_at_millis: chrono::Utc::now().timestamp_millis(),
            fs_program_bytes: program_bytes,
            comment: comment.to_string(),
            checkpoint_interval_ms,
            latest_checkpoint_epoch: 0,
            source_checkpoints: vec![],
        };
        let payload = def.encode_to_vec();
        let key = Self::build_streaming_job_key(table_name);
        self.store.put(&key, payload)?;
        info!(table = %table_name, interval_ms = checkpoint_interval_ms, "Streaming job definition persisted");
        Ok(())
    }

    pub fn remove_streaming_job(&self, table_name: &str) -> DFResult<()> {
        let key = Self::build_streaming_job_key(table_name);
        self.store.delete(&key)?;
        info!(table = %table_name, "Streaming job definition removed from store");
        Ok(())
    }

    /// Persist the globally-completed checkpoint epoch after all operators ACK.
    /// Only advances forward; stale epochs are silently ignored.
    ///
    /// `source_checkpoints` is the source-agnostic list assembled by the job coordinator via
    /// [`CheckpointAggregatorRegistry::aggregate_all`]; it is stored atomically next to
    /// `latest_checkpoint_epoch` via [`MetaStore::write_batch`].
    pub fn commit_job_checkpoint(
        &self,
        table_name: &str,
        epoch: u64,
        source_checkpoints: Vec<pb::SourceCheckpointInfo>,
    ) -> DFResult<()> {
        let key = Self::build_streaming_job_key(table_name);

        let current_payload = self.store.get(&key)?.ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(format!(
                "Cannot commit checkpoint: Streaming job '{}' not found in catalog",
                table_name
            ))
        })?;

        let mut def =
            pb::StreamingTableDefinition::decode(current_payload.as_slice()).map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Protobuf decode error: {}",
                    e
                ))
            })?;

        if epoch > def.latest_checkpoint_epoch {
            def.latest_checkpoint_epoch = epoch;
            def.source_checkpoints = source_checkpoints;
            self.store
                .write_batch(vec![(key, Some(def.encode_to_vec()))])?;
            debug!(
                table = %table_name,
                epoch = epoch,
                source_subtasks = def.source_checkpoints.len(),
                "Checkpoint metadata committed to Catalog (write_batch)"
            );
        }

        Ok(())
    }

    /// Load all persisted streaming jobs (including source checkpoint data for restore).
    pub fn load_streaming_job_definitions(&self) -> DFResult<Vec<StoredStreamingJob>> {
        let records = self.store.scan_prefix(STREAMING_JOB_KEY_PREFIX)?;
        let mut out = Vec::with_capacity(records.len());
        for (key, payload) in records {
            let def = match pb::StreamingTableDefinition::decode(payload.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        key = %key,
                        error = %e,
                        "Skipping corrupted streaming job record"
                    );
                    continue;
                }
            };
            let program = match FsProgram::decode(def.fs_program_bytes.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        table = %def.table_name,
                        error = %e,
                        "Skipping streaming job with corrupted FsProgram"
                    );
                    continue;
                }
            };
            out.push(StoredStreamingJob {
                table_name: def.table_name,
                program,
                checkpoint_interval_ms: def.checkpoint_interval_ms,
                latest_checkpoint_epoch: def.latest_checkpoint_epoch,
                source_checkpoints: def.source_checkpoints,
            });
        }
        Ok(out)
    }

    // ========================================================================
    // Catalog table persistence (CREATE TABLE / DROP TABLE)
    // ========================================================================

    pub fn add_catalog_table(&self, table: CatalogEntity) -> DFResult<()> {
        let proto_def = self.encode_catalog_table(&table)?;
        let payload = proto_def.encode_to_vec();
        let key = Self::build_store_key(table.name());

        self.store.put(&key, payload)?;
        Ok(())
    }

    pub fn has_catalog_table(&self, name: &str) -> bool {
        let key = Self::build_store_key(name);
        self.store.get(&key).ok().flatten().is_some()
    }

    pub fn drop_catalog_table(&self, table_name: &str, if_exists: bool) -> DFResult<()> {
        let key = Self::build_store_key(table_name);
        let exists = self.store.get(&key)?.is_some();
        if !exists {
            if if_exists {
                return Ok(());
            }
            return plan_err!("Table '{table_name}' not found");
        }
        self.store.delete(&key)?;
        Ok(())
    }

    pub fn restore_from_store(&self) -> DFResult<()> {
        // No-op by design: the catalog is read-through from storage.
        Ok(())
    }

    pub fn acquire_planning_context(&self) -> StreamPlanningContext {
        let mut ctx = StreamPlanningContext::new();
        let catalogs = self.load_catalog_tables_map().unwrap_or_default();
        ctx.tables.catalogs = catalogs.clone();

        for (name, table) in catalogs {
            let stream = match table.as_ref() {
                CatalogEntity::ExternalConnector(b) => match b.as_ref() {
                    ExternalTable::Source(s) => Some(StreamTable::Source {
                        name: s.name().to_string(),
                        connector: s.connector().to_string(),
                        schema: Arc::new(s.produce_physical_schema()),
                        event_time_field: s.event_time_field().map(str::to_string),
                        watermark_field: s.stream_catalog_watermark_field(),
                        with_options: s.catalog_with_options().clone(),
                    }),
                    ExternalTable::Lookup(l) => Some(StreamTable::Source {
                        name: l.name().to_string(),
                        connector: l.connector().to_string(),
                        schema: Arc::new(l.produce_physical_schema()),
                        event_time_field: None,
                        watermark_field: None,
                        with_options: l.catalog_with_options().clone(),
                    }),
                    ExternalTable::Sink(_) => None,
                },
                CatalogEntity::ComputedTable { .. } => None,
            };
            if let Some(st) = stream {
                ctx.tables.streams.insert(name, Arc::new(st));
            }
        }
        ctx
    }

    /// All persisted catalog tables, sorted by table name.
    pub fn list_catalog_tables(&self) -> DFResult<Vec<Arc<CatalogEntity>>> {
        let mut out: Vec<Arc<CatalogEntity>> =
            self.load_catalog_tables_map()?.into_values().collect();
        out.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(out)
    }

    pub fn get_catalog_table(&self, name: &str) -> DFResult<Option<Arc<CatalogEntity>>> {
        let key = UniCase::new(name.to_string());
        Ok(self.load_catalog_tables_map()?.get(&key).cloned())
    }

    pub fn add_table(&self, table: StreamTable) -> DFResult<()> {
        match table {
            StreamTable::Source {
                name,
                connector,
                schema,
                event_time_field,
                watermark_field,
                with_options,
            } => {
                let schema_specs: Vec<ColumnDescriptor> = schema
                    .fields()
                    .iter()
                    .map(|f| ColumnDescriptor::new_physical((**f).clone()))
                    .collect();
                let inferred_fields = Some(schema.fields().iter().cloned().collect());
                let physical_schema = schema.as_ref().clone();

                let connector_config = build_connector_config_for_role(
                    &connector,
                    TableRole::Ingestion,
                    &with_options,
                    &physical_schema,
                )?;

                let source = SourceTable {
                    table_identifier: name,
                    adapter_type: connector,
                    schema_specs,
                    connector_config,
                    temporal_config: TemporalPipelineConfig {
                        event_column: event_time_field,
                        watermark_strategy_column: watermark_field,
                        liveness_timeout: None,
                    },
                    key_constraints: Vec::new(),
                    payload_format: None,
                    connection_format: None,
                    description: String::new(),
                    catalog_with_options: with_options.into_iter().collect(),
                    registry_id: None,
                    inferred_fields,
                };
                self.add_catalog_table(CatalogEntity::external(ExternalTable::Source(source)))
            }
            StreamTable::Sink { name, .. } => plan_err!(
                "Persisting streaming sink '{name}' in stream catalog is no longer supported"
            ),
        }
    }

    pub fn has_stream_table(&self, name: &str) -> bool {
        self.has_catalog_table(name)
    }

    pub fn drop_table(&self, table_name: &str, if_exists: bool) -> DFResult<()> {
        self.drop_catalog_table(table_name, if_exists)
    }

    pub fn list_stream_tables(&self) -> Vec<Arc<StreamTable>> {
        self.list_catalog_tables()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|t| external_to_stream_table(t.as_ref()).map(Arc::new))
            .collect()
    }

    pub fn get_stream_table(&self, name: &str) -> Option<Arc<StreamTable>> {
        self.get_catalog_table(name)
            .ok()
            .flatten()
            .and_then(|t| external_to_stream_table(t.as_ref()).map(Arc::new))
    }

    fn encode_catalog_table(&self, table: &CatalogEntity) -> DFResult<pb::TableDefinition> {
        let table_type = match table {
            CatalogEntity::ExternalConnector(b) => match b.as_ref() {
                ExternalTable::Source(source) => {
                    let mut opts: std::collections::HashMap<String, String> =
                        source.catalog_with_options.clone().into_iter().collect();
                    opts.entry("connector".to_string())
                        .or_insert_with(|| source.connector().to_string());
                    let catalog_row = pb::CatalogSourceTable {
                        arrow_schema_ipc: CatalogCodec::encode_schema(&Arc::new(
                            source.produce_physical_schema(),
                        ))?,
                        event_time_field: source.event_time_field().map(str::to_string),
                        watermark_field: source.stream_catalog_watermark_field(),
                        with_options: opts,
                        connector: source.connector().to_string(),
                        description: source.description.clone(),
                    };
                    table_definition::TableType::ConnectorTable(catalog_row)
                }
                ExternalTable::Lookup(lookup) => {
                    let mut opts: std::collections::HashMap<String, String> =
                        lookup.catalog_with_options.clone().into_iter().collect();
                    opts.entry("connector".to_string())
                        .or_insert_with(|| lookup.connector().to_string());
                    let catalog_row = pb::CatalogSourceTable {
                        arrow_schema_ipc: CatalogCodec::encode_schema(&Arc::new(
                            lookup.produce_physical_schema(),
                        ))?,
                        event_time_field: None,
                        watermark_field: None,
                        with_options: opts,
                        connector: lookup.connector().to_string(),
                        description: lookup.description.clone(),
                    };
                    table_definition::TableType::LookupTable(catalog_row)
                }
                ExternalTable::Sink(sink) => {
                    return plan_err!(
                        "Persisting SINK table '{}' in stream catalog is not supported",
                        sink.name()
                    );
                }
            },
            CatalogEntity::ComputedTable { name, .. } => {
                return plan_err!(
                    "Persisting query-defined table '{}' is not supported by stream catalog storage",
                    name
                );
            }
        };

        Ok(pb::TableDefinition {
            table_name: table.name().to_string(),
            updated_at_millis: chrono::Utc::now().timestamp_millis(),
            table_type: Some(table_type),
        })
    }

    fn decode_catalog_source_table(
        &self,
        table_name: String,
        source_row: pb::CatalogSourceTable,
        as_lookup: bool,
    ) -> DFResult<CatalogEntity> {
        let connector = if source_row.connector.is_empty() {
            source_row
                .with_options
                .get("connector")
                .cloned()
                .unwrap_or_else(|| "stream_catalog".to_string())
        } else {
            source_row.connector.clone()
        };

        let schema = CatalogCodec::decode_schema(&source_row.arrow_schema_ipc)?;
        let schema_specs: Vec<ColumnDescriptor> = schema
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::new_physical((**f).clone()))
            .collect();
        let inferred_fields = Some(schema.fields().iter().cloned().collect());
        let physical_schema = schema.as_ref().clone();
        let catalog_with_options: BTreeMap<String, String> =
            source_row.with_options.clone().into_iter().collect();

        let role = if as_lookup {
            TableRole::Reference
        } else {
            TableRole::Ingestion
        };
        let connector_config = build_connector_config_for_role(
            &connector,
            role,
            &source_row.with_options,
            &physical_schema,
        )?;

        if as_lookup {
            Ok(CatalogEntity::external(ExternalTable::Lookup(
                LookupTable {
                    table_identifier: table_name,
                    adapter_type: connector,
                    schema_specs,
                    connector_config,
                    key_constraints: Vec::new(),
                    lookup_cache_max_bytes: None,
                    lookup_cache_ttl: None,
                    connection_format: None,
                    description: source_row.description,
                    catalog_with_options,
                    registry_id: None,
                    inferred_fields,
                },
            )))
        } else {
            let watermark_field = source_row
                .watermark_field
                .filter(|w| w != sql_field::COMPUTED_WATERMARK);
            Ok(CatalogEntity::external(ExternalTable::Source(
                SourceTable {
                    table_identifier: table_name,
                    adapter_type: connector,
                    schema_specs,
                    connector_config,
                    temporal_config: TemporalPipelineConfig {
                        event_column: source_row.event_time_field,
                        watermark_strategy_column: watermark_field,
                        liveness_timeout: None,
                    },
                    key_constraints: Vec::new(),
                    payload_format: None,
                    connection_format: None,
                    description: source_row.description,
                    catalog_with_options,
                    registry_id: None,
                    inferred_fields,
                },
            )))
        }
    }

    fn decode_catalog_table(&self, proto_def: pb::TableDefinition) -> DFResult<CatalogEntity> {
        let Some(table_type) = proto_def.table_type else {
            return internal_err!(
                "Corrupted catalog row: missing table_type for {}",
                proto_def.table_name
            );
        };

        match table_type {
            table_definition::TableType::ConnectorTable(src) => {
                self.decode_catalog_source_table(proto_def.table_name, src, false)
            }
            table_definition::TableType::LookupTable(src) => {
                self.decode_catalog_source_table(proto_def.table_name, src, true)
            }
        }
    }

    fn load_catalog_tables_map(
        &self,
    ) -> DFResult<std::collections::HashMap<crate::sql::schema::ObjectName, Arc<CatalogEntity>>>
    {
        let mut out = std::collections::HashMap::new();
        let records = self.store.scan_prefix(CATALOG_KEY_PREFIX)?;
        for (key, payload) in records {
            let proto_def = match pb::TableDefinition::decode(payload.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        catalog_key = %key,
                        error = %e,
                        "Skipping corrupted stream catalog row: protobuf decode failed"
                    );
                    continue;
                }
            };
            let table = match self.decode_catalog_table(proto_def) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        catalog_key = %key,
                        error = %e,
                        "Skipping unsupported/corrupted stream catalog row"
                    );
                    continue;
                }
            };
            let object_name = UniCase::new(table.name().to_string());
            out.insert(object_name, Arc::new(table));
        }
        Ok(out)
    }
}

fn build_connector_config_for_role<M>(
    connector: &str,
    role: TableRole,
    with_options: &M,
    physical_schema: &datafusion::arrow::datatypes::Schema,
) -> DFResult<ConnectorConfig>
where
    for<'a> &'a M: IntoIterator<Item = (&'a String, &'a String)>,
{
    let flat: std::collections::HashMap<String, String> = with_options
        .into_iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    crate::sql::connector::factory::build_connector_config_from_catalog(
        connector,
        role,
        flat,
        physical_schema,
    )
}

fn external_to_stream_table(table: &CatalogEntity) -> Option<StreamTable> {
    match table {
        CatalogEntity::ExternalConnector(b) => match b.as_ref() {
            ExternalTable::Source(s) => Some(StreamTable::Source {
                name: s.name().to_string(),
                connector: s.connector().to_string(),
                schema: Arc::new(s.produce_physical_schema()),
                event_time_field: s.event_time_field().map(str::to_string),
                watermark_field: s.stream_catalog_watermark_field(),
                with_options: s.catalog_with_options().clone(),
            }),
            ExternalTable::Lookup(l) => Some(StreamTable::Source {
                name: l.name().to_string(),
                connector: l.connector().to_string(),
                schema: Arc::new(l.produce_physical_schema()),
                event_time_field: None,
                watermark_field: None,
                with_options: l.catalog_with_options().clone(),
            }),
            ExternalTable::Sink(_) => None,
        },
        CatalogEntity::ComputedTable { .. } => None,
    }
}

pub fn restore_global_catalog_from_store() {
    let Some(mgr) = CatalogManager::try_global() else {
        return;
    };
    match mgr.restore_from_store() {
        Ok(()) => {
            let n = mgr.list_catalog_tables().map(|t| t.len()).unwrap_or(0);
            info!(catalog_tables = n, "Catalog loaded from durable store");
        }
        Err(e) => warn!("Stream catalog restore_from_store failed: {e:#}"),
    }
}

pub fn restore_streaming_jobs_from_store() {
    use crate::streaming::job::JobManager;

    let Some(catalog) = CatalogManager::try_global() else {
        warn!("CatalogManager not available; skipping streaming job restore");
        return;
    };
    let job_manager = match JobManager::global() {
        Ok(jm) => jm,
        Err(e) => {
            warn!(error = %e, "JobManager not available; skipping streaming job restore");
            return;
        }
    };

    let definitions = match catalog.load_streaming_job_definitions() {
        Ok(defs) => defs,
        Err(e) => {
            warn!(error = %e, "Failed to load streaming job definitions from store");
            return;
        }
    };

    if definitions.is_empty() {
        info!("No persisted streaming jobs to restore");
        return;
    }

    let total = definitions.len();
    info!(count = total, "Restoring persisted streaming jobs");

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            warn!(
                error = %e,
                "Failed to create Tokio runtime for streaming job restore"
            );
            return;
        }
    };
    let mut restored = 0usize;
    let mut failed = 0usize;

    for job in definitions {
        let StoredStreamingJob {
            table_name,
            program,
            checkpoint_interval_ms: interval_ms,
            latest_checkpoint_epoch: latest_epoch,
            source_checkpoints,
        } = job;
        let jm = job_manager.clone();
        let name = table_name.clone();

        let custom_interval = if interval_ms > 0 {
            Some(interval_ms)
        } else {
            None
        };
        let recovery_epoch = if latest_epoch > 0 {
            Some(latest_epoch)
        } else {
            None
        };

        match rt.block_on(jm.submit_job(
            name.clone(),
            program,
            custom_interval,
            recovery_epoch,
            source_checkpoints,
        )) {
            Ok(job_id) => {
                info!(
                    table = %table_name, job_id = %job_id,
                    epoch = latest_epoch, "Streaming job restored"
                );
                restored += 1;
            }
            Err(e) => {
                warn!(table = %table_name, error = %e, "Failed to restore streaming job");
                failed += 1;
            }
        }
    }

    info!(
        restored = restored,
        failed = failed,
        total = total,
        "Streaming job restore complete"
    );
}

#[allow(clippy::unwrap_or_default)]
pub fn planning_schema_provider() -> StreamPlanningContext {
    CatalogManager::try_global()
        .map(|m| m.acquire_planning_context())
        .unwrap_or_else(StreamPlanningContext::new)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};

    use crate::sql::connector::config::ConnectorConfig;
    use crate::sql::schema::catalog::{ExternalTable, SourceTable};
    use crate::sql::schema::column_descriptor::ColumnDescriptor;
    use crate::sql::schema::table::CatalogEntity;
    use crate::sql::schema::temporal_pipeline_config::TemporalPipelineConfig;
    use crate::stream_catalog::InMemoryMetaStore;

    use super::CatalogManager;

    fn create_test_manager() -> CatalogManager {
        CatalogManager::new(Arc::new(InMemoryMetaStore::new()))
    }

    fn make_test_source(name: &str) -> SourceTable {
        SourceTable {
            table_identifier: name.to_string(),
            adapter_type: "kafka".to_string(),
            schema_specs: vec![ColumnDescriptor::new_physical(Field::new(
                "a",
                DataType::Int32,
                false,
            ))],
            connector_config: ConnectorConfig::KafkaSource(
                protocol::function_stream_graph::KafkaSourceConfig::default(),
            ),
            temporal_config: TemporalPipelineConfig::default(),
            key_constraints: Vec::new(),
            payload_format: None,
            connection_format: None,
            description: String::new(),
            catalog_with_options: std::collections::BTreeMap::new(),
            registry_id: None,
            inferred_fields: None,
        }
    }

    #[test]
    fn drop_table_if_exists() {
        let mgr = create_test_manager();
        let source = make_test_source("t_drop");
        mgr.add_catalog_table(CatalogEntity::external(ExternalTable::Source(source)))
            .unwrap();

        mgr.drop_catalog_table("t_drop", false).unwrap();
        assert!(!mgr.has_catalog_table("t_drop"));

        mgr.drop_catalog_table("t_drop", true).unwrap();
        assert!(mgr.drop_catalog_table("nope", false).is_err());
        mgr.drop_catalog_table("nope", true).unwrap();
    }
}
