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

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, DataFusionError, Result as DFResult, TableReference, plan_err};
use datafusion::logical_expr::{
    self, BinaryExpr, Expr, Extension, LogicalPlan, Projection, TableScan,
};

use crate::sql::schema::connector_table::ConnectorTable;
use crate::sql::schema::field_spec::FieldSpec;
use crate::sql::schema::table::Table;
use crate::sql::schema::StreamSchemaProvider;
use crate::sql::extensions::remote_table::RemoteTableExtension;
use crate::sql::extensions::watermark_node::WatermarkNode;
use crate::sql::types::TIMESTAMP_FIELD;

/// Rewrites table scans into proper source nodes with projections and watermarks.
pub struct SourceRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
}

impl SourceRewriter<'_> {
    fn watermark_expression(table: &ConnectorTable) -> DFResult<Expr> {
        match table.watermark_field.clone() {
            Some(watermark_field) => table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &watermark_field {
                        return match f {
                            FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                                Some(Expr::Column(Column {
                                    relation: None,
                                    name: field.name().to_string(),
                                    spans: Default::default(),
                                }))
                            }
                            FieldSpec::Virtual { expression, .. } => Some(*expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Watermark field {watermark_field} not found"))
                }),
            None => Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: TIMESTAMP_FIELD.to_string(),
                    spans: Default::default(),
                })),
                op: logical_expr::Operator::Minus,
                right: Box::new(Expr::Literal(
                    ScalarValue::DurationNanosecond(Some(Duration::from_secs(1).as_nanos() as i64)),
                    None,
                )),
            })),
        }
    }

    fn projection_expressions(
        table: &ConnectorTable,
        qualifier: &TableReference,
        projection: &Option<Vec<usize>>,
    ) -> DFResult<Vec<Expr>> {
        let mut expressions: Vec<Expr> = table
            .fields
            .iter()
            .map(|field| match field {
                FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                    Expr::Column(Column {
                        relation: Some(qualifier.clone()),
                        name: field.name().to_string(),
                        spans: Default::default(),
                    })
                }
                FieldSpec::Virtual { field, expression } => expression
                    .clone()
                    .alias_qualified(Some(qualifier.clone()), field.name().to_string()),
            })
            .collect();

        if let Some(proj) = projection {
            expressions = proj.iter().map(|i| expressions[*i].clone()).collect();
        }

        if let Some(event_time_field) = table.event_time_field.clone() {
            let expr = table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &event_time_field {
                        return match f {
                            FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                                Some(Expr::Column(Column {
                                    relation: Some(qualifier.clone()),
                                    name: field.name().to_string(),
                                    spans: Default::default(),
                                }))
                            }
                            FieldSpec::Virtual { expression, .. } => Some(*expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Event time field {event_time_field} not found"))
                })?;

            expressions
                .push(expr.alias_qualified(Some(qualifier.clone()), TIMESTAMP_FIELD.to_string()));
        } else {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                TIMESTAMP_FIELD,
            )));
        }

        Ok(expressions)
    }

    fn projection(&self, table_scan: &TableScan, table: &ConnectorTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        // TODO: replace with TableSourceExtension when available
        let source_input = LogicalPlan::TableScan(table_scan.clone());

        Ok(LogicalPlan::Projection(Projection::try_new(
            Self::projection_expressions(table, &qualifier, &table_scan.projection)?,
            Arc::new(source_input),
        )?))
    }

    fn mutate_connector_table(
        &self,
        table_scan: &TableScan,
        table: &ConnectorTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let input = self.projection(table_scan, table)?;

        let schema = input.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                input,
                name: table_scan.table_name.to_owned(),
                schema,
                materialize: true,
            }),
        });

        let watermark_node = WatermarkNode::new(
            remote,
            table_scan.table_name.clone(),
            Self::watermark_expression(table)?,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!("failed to create watermark expression: {err}"))
        })?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        })))
    }

    fn mutate_table_from_query(
        &self,
        table_scan: &TableScan,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let column_expressions: Vec<_> = if let Some(projection) = &table_scan.projection {
            logical_plan
                .schema()
                .columns()
                .into_iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if projection.contains(&i) {
                        Some(Expr::Column(col))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            logical_plan
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect()
        };

        let target_columns: Vec<_> = table_scan.projected_schema.columns().into_iter().collect();

        let expressions = column_expressions
            .into_iter()
            .zip(target_columns)
            .map(|(expr, col)| expr.alias_qualified(col.relation, col.name))
            .collect();

        let projection = LogicalPlan::Projection(Projection::try_new_with_schema(
            expressions,
            Arc::new(logical_plan.clone()),
            table_scan.projected_schema.clone(),
        )?);

        Ok(Transformed::yes(projection))
    }
}

impl TreeNodeRewriter for SourceRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::TableScan(table_scan) = node else {
            return Ok(Transformed::no(node));
        };

        let table_name = table_scan.table_name.table();
        let table = self
            .schema_provider
            .get_catalog_table(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table {table_name} not found")))?;

        match table {
            Table::ConnectorTable(table) => self.mutate_connector_table(&table_scan, table),
            Table::LookupTable(_table) => {
                // TODO: implement LookupSource extension
                plan_err!("Lookup tables are not yet supported")
            }
            Table::TableFromQuery {
                name: _,
                logical_plan,
            } => self.mutate_table_from_query(&table_scan, logical_plan),
        }
    }
}
