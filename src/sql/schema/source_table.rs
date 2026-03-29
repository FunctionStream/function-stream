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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Column, DFSchema, Result, plan_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion::sql::planner::{PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::TableReference;
use protocol::grpc::api::ConnectorOp;
use tracing::warn;

use super::column_descriptor::ColumnDescriptor;
use super::data_encoding_format::DataEncodingFormat;
use super::schema_context::SchemaContext;
use super::table_execution_unit::{EngineDescriptor, SyncMode, TableExecutionUnit};
use super::table_role::{
    apply_adapter_specific_rules, deduce_role, serialize_backend_params,
    validate_adapter_availability, TableRole,
};
use super::temporal_pipeline_config::{resolve_temporal_logic, TemporalPipelineConfig, TemporalSpec};
use super::StreamSchemaProvider;
use crate::multifield_partial_ord;
use crate::sql::api::{ConnectionProfile, ConnectionSchema, SourceField};
use crate::sql::common::connector_options::ConnectorOptions;
use crate::sql::common::kafka_catalog::{
    KafkaConfig, KafkaConfigAuthentication, KafkaTable, KafkaTableSourceOffset, ReadMode,
    SinkCommitMode, TableType as KafkaTableType,
};
use crate::sql::common::with_option_keys as opt;
use crate::sql::common::{
    BadData, Format, Framing, FsSchema, JsonCompression, JsonFormat, OperatorConfig, RateLimit,
};
use crate::sql::schema::ConnectionType;
use crate::sql::schema::table::SqlSource;
use crate::sql::types::ProcessingMode;

/// Connector-backed catalog table (adapter / source-sink model).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceTable {
    pub registry_id: Option<i64>,
    pub adapter_type: String,
    pub table_identifier: String,
    pub role: TableRole,
    pub schema_specs: Vec<ColumnDescriptor>,
    /// Serialized runtime payload (e.g. JSON: connector + `connection_schema` + options).
    pub opaque_config: String,
    pub temporal_config: TemporalPipelineConfig,
    pub key_constraints: Vec<String>,
    pub payload_format: Option<DataEncodingFormat>,
    /// Wire [`Format`] when built from SQL `WITH` (updating mode, `ConnectionSchema`).
    pub connection_format: Option<Format>,
    pub description: String,
    pub partition_exprs: Arc<Option<Vec<Expr>>>,
    pub lookup_cache_max_bytes: Option<u64>,
    pub lookup_cache_ttl: Option<Duration>,
    pub inferred_fields: Option<Vec<FieldRef>>,
}

multifield_partial_ord!(
    SourceTable,
    registry_id,
    adapter_type,
    table_identifier,
    role,
    opaque_config,
    description,
    key_constraints,
    connection_format
);

impl SourceTable {
    #[inline]
    pub fn name(&self) -> &str {
        self.table_identifier.as_str()
    }

    pub fn new(
        table_identifier: impl Into<String>,
        connector: impl Into<String>,
        connection_type: ConnectionType,
    ) -> Self {
        Self {
            registry_id: None,
            adapter_type: connector.into(),
            table_identifier: table_identifier.into(),
            role: connection_type.into(),
            schema_specs: Vec::new(),
            opaque_config: String::new(),
            temporal_config: TemporalPipelineConfig::default(),
            key_constraints: Vec::new(),
            payload_format: None,
            connection_format: None,
            description: String::new(),
            partition_exprs: Arc::new(None),
            lookup_cache_max_bytes: None,
            lookup_cache_ttl: None,
            inferred_fields: None,
        }
    }

    #[inline]
    pub fn connector(&self) -> &str {
        self.adapter_type.as_str()
    }

    #[inline]
    pub fn connection_type(&self) -> ConnectionType {
        self.role.into()
    }

    pub fn event_time_field(&self) -> Option<&str> {
        self.temporal_config.event_column.as_deref()
    }

    pub fn watermark_field(&self) -> Option<&str> {
        self.temporal_config.watermark_strategy_column.as_deref()
    }

    pub fn idle_time(&self) -> Option<Duration> {
        self.temporal_config.liveness_timeout
    }

    pub fn initialize_from_params(
        id: &str,
        adapter: &str,
        raw_columns: Vec<ColumnDescriptor>,
        pk_list: Vec<String>,
        time_meta: Option<TemporalSpec>,
        options: &mut HashMap<String, String>,
        _schema_ctx: &dyn SchemaContext,
    ) -> Result<Self> {
        validate_adapter_availability(adapter)?;

        let encoding = DataEncodingFormat::extract_from_map(options)?;

        let mut refined_columns = apply_adapter_specific_rules(adapter, raw_columns);
        refined_columns = encoding.apply_envelope(refined_columns)?;

        let temporal_settings = resolve_temporal_logic(&refined_columns, time_meta)?;
        let finalized_config = serialize_backend_params(adapter, options)?;
        let role = deduce_role(options)?;

        if role == TableRole::Ingestion && encoding.supports_delta_updates() && pk_list.is_empty() {
            return plan_err!("CDC source requires at least one primary key");
        }

        Ok(Self {
            registry_id: None,
            adapter_type: adapter.to_string(),
            table_identifier: id.to_string(),
            role,
            schema_specs: refined_columns,
            opaque_config: finalized_config,
            temporal_config: temporal_settings,
            key_constraints: pk_list,
            payload_format: Some(encoding),
            connection_format: None,
            description: String::new(),
            partition_exprs: Arc::new(None),
            lookup_cache_max_bytes: None,
            lookup_cache_ttl: None,
            inferred_fields: None,
        })
    }

    pub fn produce_physical_schema(&self) -> Schema {
        Schema::new(
            self.schema_specs
                .iter()
                .filter(|c| !c.is_computed())
                .map(|c| c.arrow_field().clone())
                .collect::<Vec<_>>(),
        )
    }

    #[inline]
    pub fn physical_schema(&self) -> Schema {
        self.produce_physical_schema()
    }

    pub fn convert_to_execution_unit(&self) -> Result<TableExecutionUnit> {
        if self.role == TableRole::Egress {
            return plan_err!("Target [{}] is write-only", self.table_identifier);
        }

        if self.is_cdc_enabled() && self.schema_specs.iter().any(|c| c.is_computed()) {
            return plan_err!("CDC cannot be mixed with computed columns natively");
        }

        let mode = if self.is_cdc_enabled() {
            SyncMode::Incremental
        } else {
            SyncMode::AppendOnly
        };

        Ok(TableExecutionUnit {
            label: self.table_identifier.clone(),
            engine_meta: EngineDescriptor {
                engine_type: self.adapter_type.clone(),
                raw_payload: self.opaque_config.clone(),
            },
            sync_mode: mode,
            temporal_offset: self.temporal_config.clone(),
        })
    }

    #[inline]
    pub fn to_execution_unit(&self) -> Result<TableExecutionUnit> {
        self.convert_to_execution_unit()
    }

    fn is_cdc_enabled(&self) -> bool {
        self.payload_format
            .as_ref()
            .is_some_and(|f| f.supports_delta_updates())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_options(
        table_identifier: &str,
        connector_name: &str,
        temporary: bool,
        fields: Vec<ColumnDescriptor>,
        primary_keys: Vec<String>,
        watermark: Option<(String, Option<ast::Expr>)>,
        options: &mut ConnectorOptions,
        connection_profile: Option<&ConnectionProfile>,
        schema_provider: &StreamSchemaProvider,
        connection_type_override: Option<ConnectionType>,
        description: String,
    ) -> Result<Self> {
        let _ = connection_profile;

        if let Some(c) = options.pull_opt_str(opt::CONNECTOR)? {
            if c != connector_name {
                return plan_err!(
                    "WITH option `connector` is '{c}' but table uses connector '{connector_name}'"
                );
            }
        }

        validate_adapter_availability(connector_name)?;

        let inferred_empty = fields.is_empty();
        let mut columns = fields;
        columns = apply_adapter_specific_rules(connector_name, columns);

        let format = Format::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("invalid format: '{e}'")))?;

        if let Some(Format::Json(JsonFormat { compression, .. })) = &format
            && !matches!(compression, JsonCompression::Uncompressed)
            && connector_name != "filesystem"
        {
            return plan_err!("'json.compression' is only supported for the filesystem connector");
        }

        let framing = Framing::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("invalid framing: '{e}'")))?;

        if temporary
            && let Some(t) = options.insert_str(opt::TYPE, "lookup")?
            && t != "lookup"
        {
            return plan_err!(
                "Cannot have a temporary table with type '{t}'; temporary tables must be type 'lookup'"
            );
        }

        let payload_format = format.as_ref().map(DataEncodingFormat::from_connection_format);
        let encoding = payload_format.unwrap_or(DataEncodingFormat::Raw);
        columns = encoding.apply_envelope(columns)?;

        let schema_fields: Vec<SourceField> = columns
            .iter()
            .filter(|c| !c.is_computed())
            .map(|c| {
                let mut sf: SourceField = c.arrow_field().clone().try_into().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "field '{}' has a type '{:?}' that cannot be used in a connection table",
                        c.arrow_field().name(),
                        c.arrow_field().data_type()
                    ))
                })?;
                if let Some(key) = c.system_meta_key() {
                    sf.metadata_key = Some(key.to_string());
                }
                Ok(sf)
            })
            .collect::<Result<_>>()?;

        let bad_data = BadData::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("Invalid bad_data: '{e}'")))?;

        let connection_schema = ConnectionSchema::try_new(
            format.clone(),
            Some(bad_data.clone()),
            framing.clone(),
            schema_fields,
            None,
            Some(inferred_empty),
            primary_keys.iter().cloned().collect::<HashSet<_>>(),
        )
        .map_err(|e| DataFusionError::Plan(format!("could not create connection schema: {e}")))?;

        let role = if let Some(t) = connection_type_override {
            t.into()
        } else {
            match options.pull_opt_str(opt::TYPE)?.as_deref() {
                None | Some("source") => TableRole::Ingestion,
                Some("sink") => TableRole::Egress,
                Some("lookup") => TableRole::Reference,
                Some(other) => {
                    return plan_err!("invalid connection type '{other}' in WITH options");
                }
            }
        };

        let mut table = SourceTable {
            registry_id: None,
            adapter_type: connector_name.to_string(),
            table_identifier: table_identifier.to_string(),
            role,
            schema_specs: columns,
            opaque_config: String::new(),
            temporal_config: TemporalPipelineConfig::default(),
            key_constraints: Vec::new(),
            payload_format,
            connection_format: format.clone(),
            description,
            partition_exprs: Arc::new(None),
            lookup_cache_max_bytes: None,
            lookup_cache_ttl: None,
            inferred_fields: None,
        };

        if let Some(event_time_field) = options.pull_opt_field(opt::EVENT_TIME_FIELD)? {
            warn!("`event_time_field` WITH option is deprecated; use WATERMARK FOR syntax");
            table.temporal_config.event_column = Some(event_time_field);
        }

        if let Some(watermark_field) = options.pull_opt_field(opt::WATERMARK_FIELD)? {
            warn!("`watermark_field` WITH option is deprecated; use WATERMARK FOR syntax");
            table.temporal_config.watermark_strategy_column = Some(watermark_field);
        }

        if let Some((time_field, watermark_expr)) = watermark {
            let table_ref = TableReference::bare(table.table_identifier.as_str());
            let df_schema =
                DFSchema::try_from_qualified_schema(table_ref, &table.produce_physical_schema())?;

            let field = table
                .schema_specs
                .iter()
                .find(|c| c.arrow_field().name().as_str() == time_field.as_str())
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "WATERMARK FOR field `{}` does not exist in table",
                        time_field
                    )
                })?;

            if !matches!(field.arrow_field().data_type(), DataType::Timestamp(_, None)) {
                return plan_err!(
                    "WATERMARK FOR field `{time_field}` has type {}, but expected TIMESTAMP",
                    field.arrow_field().data_type()
                );
            }

            table.temporal_config.event_column = Some(time_field.clone());

            if let Some(expr) = watermark_expr {
                let logical_expr = plan_generating_expr(&expr, &df_schema, schema_provider)
                    .map_err(|e| {
                        DataFusionError::Plan(format!("could not plan watermark expression: {e}"))
                    })?;

                let (data_type, nullable) = logical_expr.data_type_and_nullable(&df_schema)?;
                if !matches!(data_type, DataType::Timestamp(_, _)) {
                    return plan_err!(
                        "the type of the WATERMARK FOR expression must be TIMESTAMP, but was {data_type}"
                    );
                }
                if nullable {
                    return plan_err!(
                        "the type of the WATERMARK FOR expression must be NOT NULL"
                    );
                }

                table.schema_specs.push(ColumnDescriptor::new_computed(
                    Field::new(
                        "__watermark",
                        logical_expr.get_type(&df_schema)?,
                        false,
                    ),
                    logical_expr,
                ));
                table.temporal_config.watermark_strategy_column = Some("__watermark".to_string());
            } else {
                table.temporal_config.watermark_strategy_column = Some(time_field);
            }
        }

        let idle_from_micros = options
            .pull_opt_i64(opt::IDLE_MICROS)?
            .filter(|t| *t > 0)
            .map(|t| Duration::from_micros(t as u64));
        let idle_from_duration = options.pull_opt_duration(opt::IDLE_TIME)?;
        table.temporal_config.liveness_timeout = idle_from_micros.or(idle_from_duration);

        table.lookup_cache_max_bytes = options.pull_opt_u64(opt::LOOKUP_CACHE_MAX_BYTES)?;

        table.lookup_cache_ttl = options.pull_opt_duration(opt::LOOKUP_CACHE_TTL)?;

        if connector_name.eq_ignore_ascii_case("kafka") {
            let physical = table.produce_physical_schema();
            let op_cfg = wire_kafka_operator_config(
                options,
                role,
                &physical,
                &format,
                bad_data,
                framing,
            )?;
            table.opaque_config = serde_json::to_string(&op_cfg).map_err(|e| {
                DataFusionError::Plan(format!("failed to serialize Kafka OperatorConfig: {e}"))
            })?;
        } else {
            let extra_opts = options.drain_remaining_string_values()?;
            let mut map = serde_json::Map::new();
            map.insert(
                opt::CONNECTOR.to_string(),
                serde_json::Value::String(connector_name.to_string()),
            );
            let schema_val = serde_json::to_value(&connection_schema).map_err(|e| {
                DataFusionError::Plan(format!("failed to serialize connection schema: {e}"))
            })?;
            map.insert(opt::CONNECTION_SCHEMA.to_string(), schema_val);
            for (k, v) in extra_opts {
                map.insert(k, serde_json::Value::String(v));
            }
            let config_root = serde_json::Value::Object(map);
            table.opaque_config = serde_json::to_string(&config_root).map_err(|e| {
                DataFusionError::Plan(format!("failed to serialize connector config: {e}"))
            })?;
        }

        if role == TableRole::Ingestion && encoding.supports_delta_updates() && primary_keys.is_empty()
        {
            return plan_err!("Debezium source must have at least one PRIMARY KEY field");
        }

        table.key_constraints = primary_keys;

        Ok(table)
    }

    pub fn has_virtual_fields(&self) -> bool {
        self.schema_specs.iter().any(|c| c.is_computed())
    }

    pub fn is_updating(&self) -> bool {
        self.connection_format
            .as_ref()
            .is_some_and(|f| f.is_updating())
            || self.payload_format == Some(DataEncodingFormat::DebeziumJson)
    }

    pub fn connector_op(&self) -> ConnectorOp {
        ConnectorOp {
            connector: self.adapter_type.clone(),
            config: self.opaque_config.clone(),
            description: self.description.clone(),
        }
    }

    pub fn processing_mode(&self) -> ProcessingMode {
        if self.is_updating() {
            ProcessingMode::Update
        } else {
            ProcessingMode::Append
        }
    }

    pub fn timestamp_override(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = self.temporal_config.event_column.clone() {
            if self.is_updating() {
                return plan_err!("can't use event_time_field with update mode");
            }
            let _field = self.get_time_column(&field_name)?;
            Ok(Some(Expr::Column(Column::from_name(field_name.as_str()))))
        } else {
            Ok(None)
        }
    }

    fn get_time_column(&self, field_name: &str) -> Result<&ColumnDescriptor> {
        self.schema_specs
            .iter()
            .find(|c| {
                c.arrow_field().name() == field_name
                    && matches!(c.arrow_field().data_type(), DataType::Timestamp(..))
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "field {field_name} not found or not a timestamp"
                ))
            })
    }

    pub fn watermark_column(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = self.temporal_config.watermark_strategy_column.clone() {
            let _field = self.get_time_column(&field_name)?;
            Ok(Some(Expr::Column(Column::from_name(field_name.as_str()))))
        } else {
            Ok(None)
        }
    }

    pub fn as_sql_source(&self) -> Result<SourceOperator> {
        match self.role {
            TableRole::Ingestion => {}
            TableRole::Egress | TableRole::Reference => {
                return plan_err!("cannot read from sink");
            }
        };

        if self.is_updating() && self.has_virtual_fields() {
            return plan_err!("can't read from a source with virtual fields and update mode.");
        }

        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        let source = SqlSource {
            id: self.registry_id,
            struct_def: self
                .schema_specs
                .iter()
                .filter(|c| !c.is_computed())
                .map(|c| Arc::new(c.arrow_field().clone()))
                .collect(),
            config: self.connector_op(),
            processing_mode: self.processing_mode(),
            idle_time: self.temporal_config.liveness_timeout,
        };

        Ok(SourceOperator {
            name: self.table_identifier.clone(),
            source,
            timestamp_override,
            watermark_column,
        })
    }
}

/// Kafka: runtime [`KafkaSourceDispatcher`] / [`KafkaSinkDispatcher`] expect [`OperatorConfig`] JSON,
/// not the legacy `{ connector, connection_schema, ... }` blob used by other adapters.
fn wire_kafka_operator_config(
    options: &mut ConnectorOptions,
    role: TableRole,
    physical_schema: &Schema,
    format: &Option<Format>,
    bad_data: BadData,
    framing: Option<Framing>,
) -> Result<OperatorConfig> {
    let bootstrap_servers = match options.pull_opt_str(opt::KAFKA_BOOTSTRAP_SERVERS)? {
        Some(s) => s,
        None => options
            .pull_opt_str(opt::KAFKA_BOOTSTRAP_SERVERS_LEGACY)?
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Kafka connector requires 'bootstrap.servers' in the WITH clause"
                )
            })?,
    };

    let topic = options
        .pull_opt_str(opt::KAFKA_TOPIC)?
        .ok_or_else(|| plan_datafusion_err!("Kafka connector requires 'topic' in the WITH clause"))?;

    let sql_format = format.clone().ok_or_else(|| {
        plan_datafusion_err!(
            "Kafka connector requires 'format' in the WITH clause (e.g. format = 'json')"
        )
    })?;

    let rate_limit = options
        .pull_opt_u64(opt::KAFKA_RATE_LIMIT_MESSAGES_PER_SECOND)?
        .map(|v| RateLimit {
            messages_per_second: v.clamp(1, u32::MAX as u64) as u32,
        });

    let value_subject = options.pull_opt_str(opt::KAFKA_VALUE_SUBJECT)?;

    let kind = match role {
        TableRole::Ingestion => {
            let offset = match options.pull_opt_str(opt::KAFKA_SCAN_STARTUP_MODE)?.as_deref() {
                Some("latest") => KafkaTableSourceOffset::Latest,
                Some("earliest") => KafkaTableSourceOffset::Earliest,
                None | Some("group-offsets") | Some("group") => KafkaTableSourceOffset::Group,
                Some(other) => {
                    return plan_err!(
                        "invalid scan.startup.mode '{other}'; expected latest, earliest, or group-offsets"
                    );
                }
            };
            let read_mode = match options.pull_opt_str(opt::KAFKA_ISOLATION_LEVEL)?.as_deref() {
                Some("read_committed") => Some(ReadMode::ReadCommitted),
                Some("read_uncommitted") => Some(ReadMode::ReadUncommitted),
                None => None,
                Some(other) => {
                    return plan_err!("invalid isolation.level '{other}'");
                }
            };
            let group_id = match options.pull_opt_str(opt::KAFKA_GROUP_ID)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_GROUP_ID_LEGACY)?,
            };
            let group_id_prefix = options.pull_opt_str(opt::KAFKA_GROUP_ID_PREFIX)?;
            KafkaTableType::Source {
                offset,
                read_mode,
                group_id,
                group_id_prefix,
            }
        }
        TableRole::Egress => {
            let commit_mode = match options.pull_opt_str(opt::KAFKA_SINK_COMMIT_MODE)?.as_deref() {
                Some("exactly-once") | Some("exactly_once") => SinkCommitMode::ExactlyOnce,
                None | Some("at-least-once") | Some("at_least_once") => SinkCommitMode::AtLeastOnce,
                Some(other) => {
                    return plan_err!("invalid sink.commit.mode '{other}'");
                }
            };
            let key_field = match options.pull_opt_str(opt::KAFKA_SINK_KEY_FIELD)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_KEY_FIELD_LEGACY)?,
            };
            let timestamp_field = match options.pull_opt_str(opt::KAFKA_SINK_TIMESTAMP_FIELD)? {
                Some(s) => Some(s),
                None => options.pull_opt_str(opt::KAFKA_TIMESTAMP_FIELD_LEGACY)?,
            };
            KafkaTableType::Sink {
                commit_mode,
                key_field,
                timestamp_field,
            }
        }
        TableRole::Reference => {
            return plan_err!("Kafka connector cannot be used as a lookup table in this path");
        }
    };

    // Role already decided; keep these out of librdkafka `connection_properties`.
    let _ = options.pull_opt_str(opt::TYPE)?;
    let _ = options.pull_opt_str(opt::CONNECTOR)?;

    let connection_properties = options.drain_remaining_string_values()?;

    let kafka_connection = KafkaConfig {
        bootstrap_servers,
        authentication: KafkaConfigAuthentication::None,
        schema_registry_enum: None,
        connection_properties,
    };

    let kafka_table = KafkaTable {
        topic,
        kind,
        client_configs: HashMap::new(),
        value_subject,
    };

    let fields: Vec<Field> = physical_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let input_schema = FsSchema::from_fields(fields);

    Ok(OperatorConfig {
        connection: serde_json::to_value(&kafka_connection).map_err(|e| {
            DataFusionError::Plan(format!("Kafka connection serialization failed: {e}"))
        })?,
        table: serde_json::to_value(&kafka_table).map_err(|e| {
            DataFusionError::Plan(format!("Kafka table serialization failed: {e}"))
        })?,
        format: Some(sql_format),
        bad_data: Some(bad_data),
        framing,
        rate_limit,
        metadata_fields: vec![],
        input_schema: Some(input_schema),
    })
}

/// Plan a SQL scalar expression against a table-qualified schema (e.g. watermark `AS` clause).
fn plan_generating_expr(
    ast: &ast::Expr,
    df_schema: &DFSchema,
    schema_provider: &StreamSchemaProvider,
) -> Result<Expr> {
    let planner = SqlToRel::new(schema_provider);
    let mut ctx = PlannerContext::new();
    planner.sql_to_expr(ast.clone(), df_schema, &mut ctx)
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub timestamp_override: Option<Expr>,
    pub watermark_column: Option<Expr>,
}
