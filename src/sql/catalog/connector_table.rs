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

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

use super::connector::{ConnectionType, ConnectorOp};
use super::field_spec::FieldSpec;
use crate::multifield_partial_ord;
use crate::sql::types::ProcessingMode;

/// Represents a table backed by an external connector (e.g., Kafka, Pulsar, NATS).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorTable {
    pub id: Option<i64>,
    pub connector: String,
    pub name: String,
    pub connection_type: ConnectionType,
    pub fields: Vec<FieldSpec>,
    pub config: String,
    pub description: String,
    pub event_time_field: Option<String>,
    pub watermark_field: Option<String>,
    pub idle_time: Option<Duration>,
    pub primary_keys: Arc<Vec<String>>,
    pub inferred_fields: Option<Vec<FieldRef>>,
    pub partition_exprs: Arc<Option<Vec<Expr>>>,
}

multifield_partial_ord!(
    ConnectorTable,
    id,
    connector,
    name,
    connection_type,
    config,
    description,
    event_time_field,
    watermark_field,
    idle_time,
    primary_keys
);

impl ConnectorTable {
    pub fn new(
        name: impl Into<String>,
        connector: impl Into<String>,
        connection_type: ConnectionType,
    ) -> Self {
        Self {
            id: None,
            connector: connector.into(),
            name: name.into(),
            connection_type,
            fields: Vec::new(),
            config: String::new(),
            description: String::new(),
            event_time_field: None,
            watermark_field: None,
            idle_time: None,
            primary_keys: Arc::new(Vec::new()),
            inferred_fields: None,
            partition_exprs: Arc::new(None),
        }
    }

    pub fn has_virtual_fields(&self) -> bool {
        self.fields.iter().any(|f| f.is_virtual())
    }

    pub fn is_updating(&self) -> bool {
        // TODO: check format for debezium/update mode
        false
    }

    pub fn physical_schema(&self) -> Schema {
        Schema::new(
            self.fields
                .iter()
                .filter(|f| !f.is_virtual())
                .map(|f| f.field().clone())
                .collect::<Vec<_>>(),
        )
    }

    pub fn connector_op(&self) -> ConnectorOp {
        ConnectorOp {
            connector: self.connector.clone(),
            config: self.config.clone(),
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
        if let Some(field_name) = &self.event_time_field {
            if self.is_updating() {
                return plan_err!("can't use event_time_field with update mode");
            }
            let _field = self.get_time_field(field_name)?;
            Ok(Some(Expr::Column(datafusion::common::Column::from_name(
                field_name,
            ))))
        } else {
            Ok(None)
        }
    }

    fn get_time_field(&self, field_name: &str) -> Result<&FieldSpec> {
        self.fields
            .iter()
            .find(|f| {
                f.field().name() == field_name
                    && matches!(
                        f.field().data_type(),
                        datafusion::arrow::datatypes::DataType::Timestamp(..)
                    )
            })
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "field {field_name} not found or not a timestamp"
                ))
            })
    }

    pub fn watermark_column(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = &self.watermark_field {
            let _field = self.get_time_field(field_name)?;
            Ok(Some(Expr::Column(datafusion::common::Column::from_name(
                field_name,
            ))))
        } else {
            Ok(None)
        }
    }

    pub fn as_sql_source(&self) -> Result<SourceOperator> {
        match self.connection_type {
            ConnectionType::Source => {}
            ConnectionType::Sink | ConnectionType::Lookup => {
                return plan_err!("cannot read from sink");
            }
        }

        if self.is_updating() && self.has_virtual_fields() {
            return plan_err!("can't read from a source with virtual fields and update mode");
        }

        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        Ok(SourceOperator {
            name: self.name.clone(),
            connector_op: self.connector_op(),
            processing_mode: self.processing_mode(),
            idle_time: self.idle_time,
            struct_fields: self
                .fields
                .iter()
                .filter(|f| !f.is_virtual())
                .map(|f| Arc::new(f.field().clone()))
                .collect(),
            timestamp_override,
            watermark_column,
        })
    }
}

/// A fully resolved source operator ready for execution graph construction.
#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub connector_op: ConnectorOp,
    pub processing_mode: ProcessingMode,
    pub idle_time: Option<Duration>,
    pub struct_fields: Vec<FieldRef>,
    pub timestamp_override: Option<Expr>,
    pub watermark_column: Option<Expr>,
}
