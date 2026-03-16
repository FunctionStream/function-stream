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

use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::sqlparser::ast::Statement;

use super::connector_table::ConnectorTable;
use super::optimizer::produce_optimized_plan;
use crate::sql::planner::StreamSchemaProvider;
use crate::sql::planner::extension::remote_table::RemoteTableExtension;
use crate::sql::planner::plan::rewrite_plan;
use crate::sql::types::DFField;

/// Represents all table types in the FunctionStream SQL catalog.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Table {
    /// A lookup table backed by an external connector.
    LookupTable(ConnectorTable),
    /// A source/sink table backed by an external connector.
    ConnectorTable(ConnectorTable),
    /// An in-memory table with an optional logical plan (for views).
    MemoryTable {
        name: String,
        fields: Vec<FieldRef>,
        logical_plan: Option<LogicalPlan>,
    },
    /// A table defined by a query (CREATE VIEW / CREATE TABLE AS SELECT).
    TableFromQuery {
        name: String,
        logical_plan: LogicalPlan,
    },
    /// A preview sink for debugging/inspection.
    PreviewSink { logical_plan: LogicalPlan },
}

impl Table {
    /// Try to construct a Table from a CREATE TABLE or CREATE VIEW statement.
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &StreamSchemaProvider,
    ) -> Result<Option<Self>> {
        use datafusion::logical_expr::{CreateMemoryTable, CreateView, DdlStatement};
        use datafusion::sql::sqlparser::ast::CreateTable;

        if let Statement::CreateTable(CreateTable {
            name,
            columns,
            query: None,
            ..
        }) = statement
        {
            let name = name.to_string();

            if columns.is_empty() {
                return plan_err!("CREATE TABLE requires at least one column");
            }

            let fields: Vec<FieldRef> = columns
                .iter()
                .map(|col| {
                    let data_type = crate::sql::types::convert_data_type(&col.data_type)
                        .map(|(dt, _)| dt)
                        .unwrap_or(datafusion::arrow::datatypes::DataType::Utf8);
                    let nullable = !col.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            datafusion::sql::sqlparser::ast::ColumnOption::NotNull
                        )
                    });
                    Arc::new(datafusion::arrow::datatypes::Field::new(
                        col.name.value.clone(),
                        data_type,
                        nullable,
                    ))
                })
                .collect();

            return Ok(Some(Table::MemoryTable {
                name,
                fields,
                logical_plan: None,
            }));
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
                let remote = RemoteTableExtension {
                    input: rewritten,
                    name: name.to_owned(),
                    schema,
                    materialize: true,
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
            Table::MemoryTable { name, .. } | Table::TableFromQuery { name, .. } => name.as_str(),
            Table::ConnectorTable(c) | Table::LookupTable(c) => c.name.as_str(),
            Table::PreviewSink { .. } => "preview",
        }
    }

    pub fn get_fields(&self) -> Vec<FieldRef> {
        match self {
            Table::MemoryTable { fields, .. } => fields.clone(),
            Table::ConnectorTable(ConnectorTable {
                fields,
                inferred_fields,
                ..
            })
            | Table::LookupTable(ConnectorTable {
                fields,
                inferred_fields,
                ..
            }) => inferred_fields.clone().unwrap_or_else(|| {
                fields
                    .iter()
                    .map(|field| field.field().clone().into())
                    .collect()
            }),
            Table::TableFromQuery { logical_plan, .. } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
            Table::PreviewSink { logical_plan } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
        }
    }

    pub fn set_inferred_fields(&mut self, fields: Vec<DFField>) -> Result<()> {
        let Table::ConnectorTable(t) = self else {
            return Ok(());
        };

        if !t.fields.is_empty() {
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

    pub fn connector_op(&self) -> Result<super::connector::ConnectorOp> {
        match self {
            Table::ConnectorTable(c) | Table::LookupTable(c) => Ok(c.connector_op()),
            Table::MemoryTable { .. } => plan_err!("can't write to a memory table"),
            Table::TableFromQuery { .. } => plan_err!("can't write to a query-defined table"),
            Table::PreviewSink { .. } => Ok(super::connector::ConnectorOp::new("preview", "")),
        }
    }

    pub fn partition_exprs(&self) -> Option<&Vec<datafusion::logical_expr::Expr>> {
        match self {
            Table::ConnectorTable(c) => (*c.partition_exprs).as_ref(),
            _ => None,
        }
    }
}
