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

use crate::analysis::rewrite_plan;
use crate::logical_node::remote_table::RemoteTableBoundaryNode;
use crate::logical_planner::optimizers::produce_optimized_plan;
use crate::schema::StreamSchemaProvider;
use crate::schema::catalog::ExternalTable;
use crate::types::{ProcessingMode, QualifiedField};
use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::sqlparser::ast::Statement;
use protocol::function_stream_graph::ConnectorOp;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CatalogEntity {
    /// Both payload variants are boxed so the enum is not padded to the largest field.
    ExternalConnector(Box<ExternalTable>),
    ComputedTable {
        name: String,
        logical_plan: Box<LogicalPlan>,
    },
}

impl CatalogEntity {
    #[inline]
    pub fn external(table: ExternalTable) -> Self {
        Self::ExternalConnector(Box::new(table))
    }

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
                Ok(Some(CatalogEntity::ComputedTable {
                    name: name.to_string(),
                    logical_plan: Box::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(remote),
                    })),
                }))
            }
            _ => Ok(None),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            CatalogEntity::ComputedTable { name, .. } => name.as_str(),
            CatalogEntity::ExternalConnector(e) => e.name(),
        }
    }

    pub fn get_fields(&self) -> Vec<FieldRef> {
        match self {
            CatalogEntity::ExternalConnector(e) => e.effective_fields(),
            CatalogEntity::ComputedTable { logical_plan, .. } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
        }
    }

    pub fn set_inferred_fields(&mut self, fields: Vec<QualifiedField>) -> Result<()> {
        let CatalogEntity::ExternalConnector(ext) = self else {
            return Ok(());
        };
        let ExternalTable::Source(t) = ext.as_mut() else {
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
            CatalogEntity::ExternalConnector(e) => Ok(e.connector_op()),
            CatalogEntity::ComputedTable { .. } => {
                plan_err!("can't write to a query-defined table")
            }
        }
    }

    pub fn partition_exprs(&self) -> Option<&Vec<datafusion::logical_expr::Expr>> {
        let CatalogEntity::ExternalConnector(ext) = self else {
            return None;
        };
        let ExternalTable::Sink(s) = ext.as_ref() else {
            return None;
        };
        (*s.partition_exprs).as_ref()
    }

    #[inline]
    pub fn as_external(&self) -> Option<&ExternalTable> {
        match self {
            CatalogEntity::ExternalConnector(e) => Some(e.as_ref()),
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
