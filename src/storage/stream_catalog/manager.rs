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
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, bail, Context};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{internal_err, plan_err, Result as DFResult};
use parking_lot::RwLock;
use prost::Message;
use protocol::storage::{self as pb, table_definition};
use tracing::{info, warn};
use unicase::UniCase;

use crate::sql::common::constants::sql_field;
use crate::sql::schema::{ObjectName, StreamPlanningContext, StreamTable};

use super::codec::CatalogCodec;
use super::meta_store::MetaStore;

const CATALOG_KEY_PREFIX: &str = "catalog:stream_table:";

#[derive(Clone, Default, Debug)]
pub struct StreamTableCatalogCache {
    pub streams: HashMap<ObjectName, Arc<StreamTable>>,
}

pub struct CatalogManager {
    store: Arc<dyn MetaStore>,
    cache: RwLock<StreamTableCatalogCache>,
}

static GLOBAL_CATALOG: OnceLock<Arc<CatalogManager>> = OnceLock::new();

impl CatalogManager {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self {
            store,
            cache: RwLock::new(StreamTableCatalogCache::default()),
        }
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

    pub fn add_table(&self, table: StreamTable) -> DFResult<()> {
        let proto_def = self.encode_table(&table)?;
        let payload = proto_def.encode_to_vec();
        let key = Self::build_store_key(table.name());

        self.store.put(&key, payload)?;

        let object_name = UniCase::new(table.name().to_string());
        self.cache.write().streams.insert(object_name, Arc::new(table));

        Ok(())
    }

    pub fn has_stream_table(&self, name: &str) -> bool {
        let object_name = UniCase::new(name.to_string());
        self.cache.read().streams.contains_key(&object_name)
    }

    pub fn drop_table(&self, table_name: &str, if_exists: bool) -> DFResult<()> {
        let object_name = UniCase::new(table_name.to_string());

        let exists = self.cache.read().streams.contains_key(&object_name);

        if !exists {
            if if_exists {
                return Ok(());
            }
            return plan_err!("Table '{table_name}' not found");
        }

        let key = Self::build_store_key(table_name);
        self.store.delete(&key)?;

        self.cache.write().streams.remove(&object_name);

        Ok(())
    }

    pub fn restore_from_store(&self) -> DFResult<()> {
        let records = self.store.scan_prefix(CATALOG_KEY_PREFIX)?;
        let mut restored = StreamTableCatalogCache::default();

        for (_key, payload) in records {
            let proto_def = pb::TableDefinition::decode(payload.as_slice()).map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Failed to decode stream catalog protobuf: {e}"
                ))
            })?;

            let table = self.decode_table(proto_def)?;
            let object_name = UniCase::new(table.name().to_string());
            restored.streams.insert(object_name, Arc::new(table));
        }

        *self.cache.write() = restored;

        Ok(())
    }

    pub fn acquire_planning_context(&self) -> StreamPlanningContext {
        let mut ctx = StreamPlanningContext::new();
        ctx.tables.streams = self.cache.read().streams.clone();
        ctx
    }

    /// All stream catalog entries (connector sources + streaming sinks), sorted by table name.
    pub fn list_stream_tables(&self) -> Vec<Arc<StreamTable>> {
        let guard = self.cache.read();
        let mut out: Vec<Arc<StreamTable>> = guard.streams.values().cloned().collect();
        out.sort_by(|a, b| a.name().cmp(b.name()));
        out
    }

    pub fn get_stream_table(&self, name: &str) -> Option<Arc<StreamTable>> {
        let key = UniCase::new(name.to_string());
        self.cache.read().streams.get(&key).cloned()
    }

    fn encode_table(&self, table: &StreamTable) -> DFResult<pb::TableDefinition> {
        let table_type = match table {
            StreamTable::Source {
                connector,
                schema,
                event_time_field,
                watermark_field,
                with_options,
                ..
            } => table_definition::TableType::Source(pb::StreamSource {
                arrow_schema_ipc: CatalogCodec::encode_schema(schema)?,
                event_time_field: event_time_field.clone(),
                watermark_field: watermark_field
                    .as_ref()
                    .filter(|w| *w != sql_field::COMPUTED_WATERMARK)
                    .cloned(),
                with_options: {
                    let mut opts: std::collections::BTreeMap<String, String> = with_options
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    opts.entry("connector".to_string())
                        .or_insert_with(|| connector.clone());
                    opts.into_iter().collect()
                },
            }),
            StreamTable::Sink { program, .. } => {
                let logical_program_bincode = CatalogCodec::encode_logical_program(program)?;
                let schema = program
                    .egress_arrow_schema()
                    .unwrap_or_else(|| Arc::new(Schema::empty()));
                table_definition::TableType::Sink(pb::StreamSink {
                    arrow_schema_ipc: CatalogCodec::encode_schema(&schema)?,
                    logical_program_bincode,
                })
            }
        };

        Ok(pb::TableDefinition {
            table_name: table.name().to_string(),
            updated_at_millis: chrono::Utc::now().timestamp_millis(),
            table_type: Some(table_type),
        })
    }

    fn decode_table(&self, proto_def: pb::TableDefinition) -> DFResult<StreamTable> {
        let Some(table_type) = proto_def.table_type else {
            return internal_err!(
                "Corrupted catalog row: missing table_type for {}",
                proto_def.table_name
            );
        };

        match table_type {
            table_definition::TableType::Source(src) => Ok(StreamTable::Source {
                name: proto_def.table_name,
                connector: src
                    .with_options
                    .get("connector")
                    .cloned()
                    .unwrap_or_else(|| "stream_catalog".to_string()),
                schema: CatalogCodec::decode_schema(&src.arrow_schema_ipc)?,
                event_time_field: src.event_time_field,
                watermark_field: src
                    .watermark_field
                    .filter(|w| w != sql_field::COMPUTED_WATERMARK),
                with_options: src.with_options.into_iter().collect(),
            }),
            table_definition::TableType::Sink(sink) => {
                if sink.logical_program_bincode.is_empty() {
                    return internal_err!(
                        "Corrupted catalog row: sink '{}' missing logical_program_bincode",
                        proto_def.table_name
                    );
                }
                let program = CatalogCodec::decode_logical_program(&sink.logical_program_bincode)?;
                Ok(StreamTable::Sink {
                    name: proto_def.table_name,
                    program,
                })
            }
        }
    }
}

pub fn restore_global_catalog_from_store() {
    let Some(mgr) = CatalogManager::try_global() else {
        return;
    };
    match mgr.restore_from_store() {
        Ok(()) => {
            let n = mgr.list_stream_tables().len();
            info!(stream_tables = n, "Stream catalog loaded from durable store");
        }
        Err(e) => warn!("Stream catalog restore_from_store failed: {e:#}"),
    }
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

    let store = std::sync::Arc::new(
        super::RocksDbMetaStore::open(&path).with_context(|| {
            format!(
                "Failed to open stream catalog RocksDB at {}",
                path.display()
            )
        })?,
    );

    CatalogManager::init_global(store).context("Stream catalog (CatalogManager) init failed")
}

pub fn planning_schema_provider() -> StreamPlanningContext {
    CatalogManager::try_global()
        .map(|m| m.acquire_planning_context())
        .unwrap_or_else(StreamPlanningContext::new)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use crate::sql::logical_node::logical::LogicalProgram;
    use crate::sql::schema::StreamTable;
    use crate::storage::stream_catalog::{InMemoryMetaStore, MetaStore};

    use super::CatalogManager;

    fn create_test_manager() -> CatalogManager {
        CatalogManager::new(Arc::new(InMemoryMetaStore::new()))
    }

    #[test]
    fn add_table_roundtrip_snapshot() {
        let mgr = create_test_manager();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let table = StreamTable::Source {
            name: "t1".into(),
            connector: "stream_catalog".into(),
            schema: Arc::clone(&schema),
            event_time_field: Some("ts".into()),
            watermark_field: None,
            with_options: BTreeMap::new(),
        };

        mgr.add_table(table).unwrap();

        let ctx = mgr.acquire_planning_context();
        let got = ctx.get_stream_table("t1").expect("table present");

        assert_eq!(got.name(), "t1");

        if let StreamTable::Source {
            event_time_field,
            watermark_field,
            ..
        } = got.as_ref()
        {
            assert_eq!(event_time_field.as_deref(), Some("ts"));
            assert!(watermark_field.is_none());
        } else {
            panic!("expected Source");
        }
    }

    #[test]
    fn add_table_roundtrip_with_options() {
        let mgr = create_test_manager();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let mut opts = BTreeMap::new();
        opts.insert("connector".to_string(), "kafka".to_string());
        opts.insert("topic".to_string(), "my-topic".to_string());

        let table = StreamTable::Source {
            name: "t_with".into(),
            connector: "kafka".into(),
            schema,
            event_time_field: None,
            watermark_field: None,
            with_options: opts.clone(),
        };

        mgr.add_table(table).unwrap();

        let ctx = mgr.acquire_planning_context();
        let got = ctx.get_stream_table("t_with").expect("table present");

        if let StreamTable::Source { with_options, .. } = got.as_ref() {
            assert_eq!(with_options, &opts);
        } else {
            panic!("expected Source");
        }
    }

    #[test]
    fn drop_table_if_exists() {
        let mgr = create_test_manager();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        mgr.add_table(StreamTable::Source {
            name: "t_drop".into(),
            connector: "stream_catalog".into(),
            schema,
            event_time_field: None,
            watermark_field: None,
            with_options: BTreeMap::new(),
        })
        .unwrap();

        mgr.drop_table("t_drop", false).unwrap();
        assert!(!mgr.has_stream_table("t_drop"));

        mgr.drop_table("t_drop", true).unwrap();
        assert!(mgr.drop_table("nope", false).is_err());
        mgr.drop_table("nope", true).unwrap();
    }

    #[test]
    fn restore_from_store_rebuilds_cache() {
        let store: Arc<dyn MetaStore> = Arc::new(InMemoryMetaStore::new());

        let mgr_a = CatalogManager::new(Arc::clone(&store));

        mgr_a
            .add_table(StreamTable::Sink {
                name: "sink1".into(),
                program: LogicalProgram::default(),
            })
            .unwrap();

        let mgr_b = CatalogManager::new(store);
        mgr_b.restore_from_store().unwrap();

        let ctx = mgr_b.acquire_planning_context();
        assert!(ctx.get_stream_table("sink1").is_some());
    }
}
