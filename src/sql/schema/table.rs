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

use super::source_table::SourceTable;
use crate::sql::analysis::rewrite_plan;
use crate::sql::extensions::remote_table::RemoteTableBoundaryNode;
use crate::sql::logical_planner::optimizers::produce_optimized_plan;
use crate::sql::schema::StreamSchemaProvider;
use crate::sql::types::{DFField, ProcessingMode};
use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::sqlparser::ast::Statement;
use protocol::grpc::api::ConnectorOp;
use std::sync::Arc;
use std::time::Duration;

/// Represents all table types in the FunctionStream SQL catalog.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Table {
    /// A lookup table backed by an external connector.
    LookupTable(SourceTable),
    /// A source/sink table backed by an external connector.
    ConnectorTable(SourceTable),
    /// A table defined by a query (CREATE VIEW / CREATE TABLE AS SELECT).
    TableFromQuery {
        name: String,
        logical_plan: LogicalPlan,
    },
}

impl Table {
    /// Try to construct a Table from a CREATE TABLE or CREATE VIEW statement.
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &StreamSchemaProvider,
    ) -> Result<Option<Self>> {
        use datafusion::logical_expr::{CreateMemoryTable, CreateView, DdlStatement};
        use datafusion::sql::sqlparser::ast::CreateTable;

        if let Statement::CreateTable(CreateTable { query: None, .. }) = statement {
            return plan_err!(
                "CREATE TABLE without AS SELECT is not supported; use CREATE TABLE ... AS SELECT or a connector table"
            );
        }

        match produce_optimized_plan(statement, schema_provider) {
            Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView { name, input, .. })))
            | Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                ..
            }))) => {
                let rewritten = rewrite_plan(input.as_ref().clone(), schema_provider)?;
                let schema = rewritten.schema().clone();
                let remote = RemoteTableBoundaryNode {
                    upstream_plan: rewritten,
                    table_identifier: name.to_owned(),
                    resolved_schema: schema,
                    requires_materialization: true,
                };
                Ok(Some(Table::TableFromQuery {
                    name: name.to_string(),
                    logical_plan: LogicalPlan::Extension(Extension {
                        node: Arc::new(remote),
                    }),
                }))
            }
            _ => Ok(None),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Table::TableFromQuery { name, .. } => name.as_str(),
            Table::ConnectorTable(c) | Table::LookupTable(c) => c.name(),
        }
    }

    pub fn get_fields(&self) -> Vec<FieldRef> {
        match self {
            Table::ConnectorTable(SourceTable {
                schema_specs,
                inferred_fields,
                ..
            })
            | Table::LookupTable(SourceTable {
                schema_specs,
                inferred_fields,
                ..
            }) => inferred_fields.clone().unwrap_or_else(|| {
                schema_specs
                    .iter()
                    .map(|c| Arc::new(c.arrow_field().clone()))
                    .collect()
            }),
            Table::TableFromQuery { logical_plan, .. } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
        }
    }

    pub fn set_inferred_fields(&mut self, fields: Vec<DFField>) -> Result<()> {
        let Table::ConnectorTable(t) = self else {
            return Ok(());
        };

        if !t.schema_specs.is_empty() {
            return Ok(());
        }

        if let Some(existing) = &t.inferred_fields {
            let matches = existing.len() == fields.len()
                && existing
                    .iter()
                    .zip(&fields)
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());

            if !matches {
                return plan_err!("all inserts into a table must share the same schema");
            }
        }

        let fields: Vec<_> = fields.into_iter().map(|f| f.field().clone()).collect();
        t.inferred_fields.replace(fields);

        Ok(())
    }

    pub fn connector_op(&self) -> Result<ConnectorOp> {
        match self {
            Table::ConnectorTable(c) | Table::LookupTable(c) => Ok(c.connector_op()),
            Table::TableFromQuery { .. } => plan_err!("can't write to a query-defined table"),
        }
    }

    pub fn partition_exprs(&self) -> Option<&Vec<datafusion::logical_expr::Expr>> {
        match self {
            Table::ConnectorTable(c) => (*c.partition_exprs).as_ref(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: Vec<FieldRef>,
    pub config: ConnectorOp,
    pub processing_mode: ProcessingMode,
    pub idle_time: Option<Duration>,
}
