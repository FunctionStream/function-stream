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

//! External connector catalog: [`ExternalTable`] as [`SourceTable`] | [`SinkTable`] | [`LookupTable`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Column, Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use protocol::function_stream_graph::ConnectorOp;

use super::column_descriptor::ColumnDescriptor;
use super::data_encoding_format::DataEncodingFormat;
use super::table::SqlSource;
use super::temporal_pipeline_config::TemporalPipelineConfig;
use crate::common::constants::sql_field;
use crate::common::{Format, FsSchema};
use crate::connector::config::ConnectorConfig;
use crate::multifield_partial_ord;
use crate::types::ProcessingMode;

#[derive(Debug, Clone)]
pub struct EngineDescriptor {
    pub engine_type: String,
    pub raw_payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncMode {
    AppendOnly,
    Incremental,
}

#[derive(Debug, Clone)]
pub struct TableExecutionUnit {
    pub label: String,
    pub engine_meta: EngineDescriptor,
    pub sync_mode: SyncMode,
    pub temporal_offset: TemporalPipelineConfig,
}

/// The only legal shape an external-connector catalog row can take.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExternalTable {
    Source(SourceTable),
    Sink(SinkTable),
    Lookup(LookupTable),
}

impl ExternalTable {
    #[inline]
    pub fn name(&self) -> &str {
        match self {
            ExternalTable::Source(t) => t.table_identifier.as_str(),
            ExternalTable::Sink(t) => t.table_identifier.as_str(),
            ExternalTable::Lookup(t) => t.table_identifier.as_str(),
        }
    }

    #[inline]
    pub fn adapter_type(&self) -> &str {
        match self {
            ExternalTable::Source(t) => t.adapter_type.as_str(),
            ExternalTable::Sink(t) => t.adapter_type.as_str(),
            ExternalTable::Lookup(t) => t.adapter_type.as_str(),
        }
    }

    #[inline]
    pub fn description(&self) -> &str {
        match self {
            ExternalTable::Source(t) => t.description.as_str(),
            ExternalTable::Sink(t) => t.description.as_str(),
            ExternalTable::Lookup(t) => t.description.as_str(),
        }
    }

    #[inline]
    pub fn schema_specs(&self) -> &[ColumnDescriptor] {
        match self {
            ExternalTable::Source(t) => &t.schema_specs,
            ExternalTable::Sink(t) => &t.schema_specs,
            ExternalTable::Lookup(t) => &t.schema_specs,
        }
    }

    #[inline]
    pub fn connector_config(&self) -> &ConnectorConfig {
        match self {
            ExternalTable::Source(t) => &t.connector_config,
            ExternalTable::Sink(t) => &t.connector_config,
            ExternalTable::Lookup(t) => &t.connector_config,
        }
    }

    #[inline]
    pub fn key_constraints(&self) -> &[String] {
        match self {
            ExternalTable::Source(t) => &t.key_constraints,
            ExternalTable::Sink(t) => &t.key_constraints,
            ExternalTable::Lookup(t) => &t.key_constraints,
        }
    }

    #[inline]
    pub fn connection_format(&self) -> Option<&Format> {
        match self {
            ExternalTable::Source(t) => t.connection_format.as_ref(),
            ExternalTable::Sink(t) => t.connection_format.as_ref(),
            ExternalTable::Lookup(t) => t.connection_format.as_ref(),
        }
    }

    #[inline]
    pub fn catalog_with_options(&self) -> &BTreeMap<String, String> {
        match self {
            ExternalTable::Source(t) => &t.catalog_with_options,
            ExternalTable::Sink(t) => &t.catalog_with_options,
            ExternalTable::Lookup(t) => &t.catalog_with_options,
        }
    }

    pub fn produce_physical_schema(&self) -> Schema {
        Schema::new(
            self.schema_specs()
                .iter()
                .filter(|c| !c.is_computed())
                .map(|c| c.arrow_field().clone())
                .collect::<Vec<_>>(),
        )
    }

    pub fn connector_op(&self) -> ConnectorOp {
        let physical = self.produce_physical_schema();
        let fields: Vec<Field> = physical
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let fs_schema = FsSchema::from_fields(fields);

        ConnectorOp {
            connector: self.adapter_type().to_string(),
            fs_schema: Some(fs_schema.into()),
            name: self.name().to_string(),
            description: self.description().to_string(),
            config: Some(self.connector_config().to_proto_config()),
        }
    }

    #[inline]
    pub fn is_updating(&self) -> bool {
        match self {
            ExternalTable::Source(t) => t.is_updating(),
            ExternalTable::Sink(t) => t
                .connection_format
                .as_ref()
                .is_some_and(|f| f.is_updating()),
            ExternalTable::Lookup(_) => false,
        }
    }

    /// Variant-agnostic view of "persisted Arrow fields post-planning".
    /// Only Source / Lookup track inferred schema — Sinks derive theirs from the upstream plan.
    pub fn effective_fields(&self) -> Vec<FieldRef> {
        match self {
            ExternalTable::Source(t) => t.effective_fields(),
            ExternalTable::Sink(t) => t.effective_fields(),
            ExternalTable::Lookup(t) => t.effective_fields(),
        }
    }

    #[inline]
    pub fn as_source(&self) -> Option<&SourceTable> {
        match self {
            ExternalTable::Source(t) => Some(t),
            _ => None,
        }
    }

    #[inline]
    pub fn as_sink(&self) -> Option<&SinkTable> {
        match self {
            ExternalTable::Sink(t) => Some(t),
            _ => None,
        }
    }

    #[inline]
    pub fn as_lookup(&self) -> Option<&LookupTable> {
        match self {
            ExternalTable::Lookup(t) => Some(t),
            _ => None,
        }
    }
}

/// Ingress external connector (`CREATE TABLE ... WITH (type='source', ...)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceTable {
    pub table_identifier: String,
    pub adapter_type: String,
    pub schema_specs: Vec<ColumnDescriptor>,
    pub connector_config: ConnectorConfig,
    pub temporal_config: TemporalPipelineConfig,
    pub key_constraints: Vec<String>,
    pub payload_format: Option<DataEncodingFormat>,
    pub connection_format: Option<Format>,
    pub description: String,
    pub catalog_with_options: BTreeMap<String, String>,

    // Planner / catalog; not in SQL text.
    pub registry_id: Option<i64>,
    pub inferred_fields: Option<Vec<FieldRef>>,
}

multifield_partial_ord!(
    SourceTable,
    registry_id,
    adapter_type,
    table_identifier,
    description,
    key_constraints,
    connection_format,
    catalog_with_options
);

impl SourceTable {
    #[inline]
    pub fn name(&self) -> &str {
        self.table_identifier.as_str()
    }

    #[inline]
    pub fn connector(&self) -> &str {
        self.adapter_type.as_str()
    }

    pub fn event_time_field(&self) -> Option<&str> {
        self.temporal_config.event_column.as_deref()
    }

    pub fn watermark_field(&self) -> Option<&str> {
        self.temporal_config.watermark_strategy_column.as_deref()
    }

    /// Watermark column safe to persist to the stream catalog. Omits the
    /// generated `__watermark` column — that is only resolvable at compile
    /// time, the catalog round-trip cannot reconstruct it.
    pub fn stream_catalog_watermark_field(&self) -> Option<String> {
        self.temporal_config
            .watermark_strategy_column
            .as_deref()
            .filter(|w| *w != sql_field::COMPUTED_WATERMARK)
            .map(str::to_string)
    }

    #[inline]
    pub fn catalog_with_options(&self) -> &BTreeMap<String, String> {
        &self.catalog_with_options
    }

    pub fn idle_time(&self) -> Option<Duration> {
        self.temporal_config.liveness_timeout
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

    pub fn effective_fields(&self) -> Vec<FieldRef> {
        self.inferred_fields.clone().unwrap_or_else(|| {
            self.schema_specs
                .iter()
                .map(|c| Arc::new(c.arrow_field().clone()))
                .collect()
        })
    }

    pub fn convert_to_execution_unit(&self) -> Result<TableExecutionUnit> {
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
                raw_payload: String::new(),
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
        let physical = self.produce_physical_schema();
        let fields: Vec<Field> = physical
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let fs_schema = FsSchema::from_fields(fields);

        ConnectorOp {
            connector: self.adapter_type.clone(),
            fs_schema: Some(fs_schema.into()),
            name: self.table_identifier.clone(),
            description: self.description.clone(),
            config: Some(self.connector_config.to_proto_config()),
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
                DataFusionError::Plan(format!("field {field_name} not found or not a timestamp"))
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

/// Egress external connector, or the sink of `CREATE STREAMING TABLE ... AS SELECT`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkTable {
    pub table_identifier: String,
    pub adapter_type: String,
    pub schema_specs: Vec<ColumnDescriptor>,
    pub connector_config: ConnectorConfig,
    pub partition_exprs: Arc<Option<Vec<Expr>>>,
    pub key_constraints: Vec<String>,
    pub connection_format: Option<Format>,
    pub description: String,
    pub catalog_with_options: BTreeMap<String, String>,
}

multifield_partial_ord!(
    SinkTable,
    adapter_type,
    table_identifier,
    description,
    key_constraints,
    connection_format,
    catalog_with_options
);

impl SinkTable {
    #[inline]
    pub fn name(&self) -> &str {
        self.table_identifier.as_str()
    }

    #[inline]
    pub fn connector(&self) -> &str {
        self.adapter_type.as_str()
    }

    #[inline]
    pub fn catalog_with_options(&self) -> &BTreeMap<String, String> {
        &self.catalog_with_options
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

    pub fn effective_fields(&self) -> Vec<FieldRef> {
        self.schema_specs
            .iter()
            .map(|c| Arc::new(c.arrow_field().clone()))
            .collect()
    }

    pub fn is_updating(&self) -> bool {
        self.connection_format
            .as_ref()
            .is_some_and(|f| f.is_updating())
    }

    pub fn connector_op(&self) -> ConnectorOp {
        let physical = self.produce_physical_schema();
        let fields: Vec<Field> = physical
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let fs_schema = FsSchema::from_fields(fields);

        ConnectorOp {
            connector: self.adapter_type.clone(),
            fs_schema: Some(fs_schema.into()),
            name: self.table_identifier.clone(),
            description: self.description.clone(),
            config: Some(self.connector_config.to_proto_config()),
        }
    }
}

/// Lookup-join only; not a scan source.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupTable {
    pub table_identifier: String,
    pub adapter_type: String,
    pub schema_specs: Vec<ColumnDescriptor>,
    pub connector_config: ConnectorConfig,
    pub key_constraints: Vec<String>,
    pub lookup_cache_max_bytes: Option<u64>,
    pub lookup_cache_ttl: Option<Duration>,
    pub connection_format: Option<Format>,
    pub description: String,
    pub catalog_with_options: BTreeMap<String, String>,

    pub registry_id: Option<i64>,
    pub inferred_fields: Option<Vec<FieldRef>>,
}

multifield_partial_ord!(
    LookupTable,
    registry_id,
    adapter_type,
    table_identifier,
    description,
    key_constraints,
    connection_format,
    catalog_with_options
);

impl LookupTable {
    #[inline]
    pub fn name(&self) -> &str {
        self.table_identifier.as_str()
    }

    #[inline]
    pub fn connector(&self) -> &str {
        self.adapter_type.as_str()
    }

    #[inline]
    pub fn catalog_with_options(&self) -> &BTreeMap<String, String> {
        &self.catalog_with_options
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

    pub fn effective_fields(&self) -> Vec<FieldRef> {
        self.inferred_fields.clone().unwrap_or_else(|| {
            self.schema_specs
                .iter()
                .map(|c| Arc::new(c.arrow_field().clone()))
                .collect()
        })
    }

    pub fn connector_op(&self) -> ConnectorOp {
        let physical = self.produce_physical_schema();
        let fields: Vec<Field> = physical
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let fs_schema = FsSchema::from_fields(fields);

        ConnectorOp {
            connector: self.adapter_type.clone(),
            fs_schema: Some(fs_schema.into()),
            name: self.table_identifier.clone(),
            description: self.description.clone(),
            config: Some(self.connector_config.to_proto_config()),
        }
    }
}

/// [`SourceTable`] as an ingestion logical node input.
#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub timestamp_override: Option<Expr>,
    pub watermark_column: Option<Expr>,
}
