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

use std::sync::{Arc, OnceLock};

use anyhow::{Context, anyhow, bail};
use datafusion::common::{Result as DFResult, internal_err, plan_err};
use prost::Message;
use protocol::function_stream_graph::FsProgram;
use protocol::storage::{self as pb, table_definition};
use tracing::{debug, info, warn};
use unicase::UniCase;

use crate::sql::common::constants::sql_field;
use crate::sql::schema::column_descriptor::ColumnDescriptor;
use crate::sql::schema::connection_type::ConnectionType;
use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::table::Table as CatalogTable;
use crate::sql::schema::{StreamPlanningContext, StreamTable};

use super::codec::CatalogCodec;
use super::meta_store::MetaStore;

const CATALOG_KEY_PREFIX: &str = "catalog:stream_table:";
const STREAMING_JOB_KEY_PREFIX: &str = "streaming_job:";

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
    pub fn commit_job_checkpoint(&self, table_name: &str, epoch: u64) -> DFResult<()> {
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
            let new_payload = def.encode_to_vec();
            self.store.put(&key, new_payload)?;
            debug!(table = %table_name, epoch = epoch, "Checkpoint metadata committed to Catalog");
        }

        Ok(())
    }

    /// Returns (table_name, program, checkpoint_interval_ms, latest_checkpoint_epoch).
    pub fn load_streaming_job_definitions(&self) -> DFResult<Vec<(String, FsProgram, u64, u64)>> {
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
            out.push((
                def.table_name,
                program,
                def.checkpoint_interval_ms,
                def.latest_checkpoint_epoch,
            ));
        }
        Ok(out)
    }

    // ========================================================================
    // Catalog table persistence (CREATE TABLE / DROP TABLE)
    // ========================================================================

    pub fn add_catalog_table(&self, table: CatalogTable) -> DFResult<()> {
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
            let source = match table.as_ref() {
                CatalogTable::ConnectorTable(s) | CatalogTable::LookupTable(s) => s,
                CatalogTable::TableFromQuery { .. } => continue,
            };

            let schema = Arc::new(source.produce_physical_schema());
            ctx.tables.streams.insert(
                name,
                Arc::new(StreamTable::Source {
                    name: source.name().to_string(),
                    connector: source.connector().to_string(),
                    schema,
                    event_time_field: source.event_time_field().map(str::to_string),
                    watermark_field: source.stream_catalog_watermark_field(),
                    with_options: source.catalog_with_options().clone(),
                }),
            );
        }
        ctx
    }

    /// All persisted catalog tables, sorted by table name.
    pub fn list_catalog_tables(&self) -> DFResult<Vec<Arc<CatalogTable>>> {
        let mut out: Vec<Arc<CatalogTable>> =
            self.load_catalog_tables_map()?.into_values().collect();
        out.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(out)
    }

    pub fn get_catalog_table(&self, name: &str) -> DFResult<Option<Arc<CatalogTable>>> {
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
                let mut source = SourceTable::new(name, connector, ConnectionType::Source);
                source.schema_specs = schema
                    .fields()
                    .iter()
                    .map(|f| ColumnDescriptor::new_physical((**f).clone()))
                    .collect();
                source.inferred_fields = Some(schema.fields().iter().cloned().collect());
                source.temporal_config.event_column = event_time_field;
                source.temporal_config.watermark_strategy_column = watermark_field;
                source.catalog_with_options = with_options;
                self.add_catalog_table(CatalogTable::ConnectorTable(source))
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
            .filter_map(|t| match t.as_ref() {
                CatalogTable::ConnectorTable(s) | CatalogTable::LookupTable(s) => {
                    Some(Arc::new(StreamTable::Source {
                        name: s.name().to_string(),
                        connector: s.connector().to_string(),
                        schema: Arc::new(s.produce_physical_schema()),
                        event_time_field: s.event_time_field().map(str::to_string),
                        watermark_field: s.stream_catalog_watermark_field(),
                        with_options: s.catalog_with_options().clone(),
                    }))
                }
                CatalogTable::TableFromQuery { .. } => None,
            })
            .collect()
    }

    pub fn get_stream_table(&self, name: &str) -> Option<Arc<StreamTable>> {
        self.get_catalog_table(name)
            .ok()
            .flatten()
            .and_then(|t| match t.as_ref() {
                CatalogTable::ConnectorTable(s) | CatalogTable::LookupTable(s) => {
                    Some(Arc::new(StreamTable::Source {
                        name: s.name().to_string(),
                        connector: s.connector().to_string(),
                        schema: Arc::new(s.produce_physical_schema()),
                        event_time_field: s.event_time_field().map(str::to_string),
                        watermark_field: s.stream_catalog_watermark_field(),
                        with_options: s.catalog_with_options().clone(),
                    }))
                }
                CatalogTable::TableFromQuery { .. } => None,
            })
    }

    fn encode_catalog_table(&self, table: &CatalogTable) -> DFResult<pb::TableDefinition> {
        let table_type = match table {
            CatalogTable::ConnectorTable(source) | CatalogTable::LookupTable(source) => {
                let mut opts = source.catalog_with_options().clone();
                opts.entry("connector".to_string())
                    .or_insert_with(|| source.connector().to_string());
                let catalog_row = pb::CatalogSourceTable {
                    arrow_schema_ipc: CatalogCodec::encode_schema(&Arc::new(
                        source.produce_physical_schema(),
                    ))?,
                    event_time_field: source.event_time_field().map(str::to_string),
                    watermark_field: source.stream_catalog_watermark_field(),
                    with_options: opts.into_iter().collect(),
                    connector: source.connector().to_string(),
                    description: source.description.clone(),
                };
                if matches!(table, CatalogTable::LookupTable(_)) {
                    table_definition::TableType::LookupTable(catalog_row)
                } else {
                    table_definition::TableType::ConnectorTable(catalog_row)
                }
            }
            CatalogTable::TableFromQuery { name, .. } => {
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
    ) -> DFResult<CatalogTable> {
        let connector = if source_row.connector.is_empty() {
            source_row
                .with_options
                .get("connector")
                .cloned()
                .unwrap_or_else(|| "stream_catalog".to_string())
        } else {
            source_row.connector.clone()
        };
        let mut source = SourceTable::new(
            table_name,
            connector,
            if as_lookup {
                ConnectionType::Lookup
            } else {
                ConnectionType::Source
            },
        );
        let schema = CatalogCodec::decode_schema(&source_row.arrow_schema_ipc)?;
        source.schema_specs = schema
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::new_physical((**f).clone()))
            .collect();
        source.inferred_fields = Some(schema.fields().iter().cloned().collect());
        source.temporal_config.event_column = source_row.event_time_field;
        source.temporal_config.watermark_strategy_column = source_row
            .watermark_field
            .filter(|w| w != sql_field::COMPUTED_WATERMARK);
        source.catalog_with_options = source_row.with_options.into_iter().collect();
        source.description = source_row.description;

        // Rebuild strongly-typed ConnectorConfig from persisted WITH options.
        if source.connector().eq_ignore_ascii_case("kafka") {
            use crate::sql::schema::ConnectorConfig;
            use crate::sql::schema::kafka_operator_config::build_kafka_proto_config_from_string_map;
            let opts_map: std::collections::HashMap<String, String> = source
                .catalog_with_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let physical = source.produce_physical_schema();
            if let Ok(proto_cfg) = build_kafka_proto_config_from_string_map(opts_map, &physical) {
                source.connector_config = match proto_cfg {
                    protocol::function_stream_graph::connector_op::Config::KafkaSource(cfg) => {
                        ConnectorConfig::KafkaSource(cfg)
                    }
                    protocol::function_stream_graph::connector_op::Config::KafkaSink(cfg) => {
                        ConnectorConfig::KafkaSink(cfg)
                    }
                    protocol::function_stream_graph::connector_op::Config::Generic(g) => {
                        ConnectorConfig::Generic(g.properties)
                    }
                };
            }
        } else {
            use crate::sql::schema::ConnectorConfig;
            source.connector_config = ConnectorConfig::Generic(
                source
                    .catalog_with_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
        }

        if as_lookup {
            Ok(CatalogTable::LookupTable(source))
        } else {
            Ok(CatalogTable::ConnectorTable(source))
        }
    }

    fn decode_catalog_table(&self, proto_def: pb::TableDefinition) -> DFResult<CatalogTable> {
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
    ) -> DFResult<std::collections::HashMap<crate::sql::schema::ObjectName, Arc<CatalogTable>>>
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
    use crate::runtime::streaming::job::JobManager;

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

    for (table_name, fs_program, interval_ms, latest_epoch) in definitions {
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

        match rt.block_on(jm.submit_job(name.clone(), fs_program, custom_interval, recovery_epoch))
        {
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

pub fn initialize_stream_catalog(config: &crate::config::GlobalConfig) -> anyhow::Result<()> {
    if !config.stream_catalog.persist {
        return CatalogManager::init_global_in_memory()
            .context("Stream catalog (CatalogManager) in-memory init failed");
    }

    let path = config
        .stream_catalog
        .db_path
        .as_ref()
        .map(|p| crate::config::resolve_path(p))
        .unwrap_or_else(|| crate::config::get_data_dir().join("stream_catalog"));

    std::fs::create_dir_all(&path).with_context(|| {
        format!(
            "Failed to create stream catalog directory {}",
            path.display()
        )
    })?;

    let store = std::sync::Arc::new(super::RocksDbMetaStore::open(&path).with_context(|| {
        format!(
            "Failed to open stream catalog RocksDB at {}",
            path.display()
        )
    })?);

    CatalogManager::init_global(store).context("Stream catalog (CatalogManager) init failed")
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

    use crate::sql::schema::column_descriptor::ColumnDescriptor;
    use crate::sql::schema::connection_type::ConnectionType;
    use crate::sql::schema::source_table::SourceTable;
    use crate::sql::schema::table::Table as CatalogTable;
    use crate::storage::stream_catalog::InMemoryMetaStore;

    use super::CatalogManager;

    fn create_test_manager() -> CatalogManager {
        CatalogManager::new(Arc::new(InMemoryMetaStore::new()))
    }

    #[test]
    fn add_table_roundtrip_snapshot() {
        let mgr = create_test_manager();
        let mut source = SourceTable::new("t1", "kafka", ConnectionType::Source);
        source.schema_specs = vec![ColumnDescriptor::new_physical(Field::new(
            "a",
            DataType::Int32,
            false,
        ))];
        source.temporal_config.event_column = Some("ts".into());
        let table = CatalogTable::ConnectorTable(source);

        mgr.add_catalog_table(table).unwrap();

        let got = mgr.get_catalog_table("t1").unwrap().expect("table present");
        assert_eq!(got.name(), "t1");
    }

    #[test]
    fn drop_table_if_exists() {
        let mgr = create_test_manager();
        let mut source = SourceTable::new("t_drop", "kafka", ConnectionType::Source);
        source.schema_specs = vec![ColumnDescriptor::new_physical(Field::new(
            "a",
            DataType::Int32,
            false,
        ))];
        mgr.add_catalog_table(CatalogTable::ConnectorTable(source))
            .unwrap();

        mgr.drop_catalog_table("t_drop", false).unwrap();
        assert!(!mgr.has_catalog_table("t_drop"));

        mgr.drop_catalog_table("t_drop", true).unwrap();
        assert!(mgr.drop_catalog_table("nope", false).is_err());
        mgr.drop_catalog_table("nope", true).unwrap();
    }
}
