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

use crate::sql::common::UPDATING_META_FIELD;
use crate::sql::logical_node::debezium::UnrollDebeziumPayloadNode;
use crate::sql::logical_node::remote_table::RemoteTableBoundaryNode;
use crate::sql::logical_node::table_source::StreamIngestionNode;
use crate::sql::logical_node::watermark_node::EventTimeWatermarkNode;
use crate::sql::schema::ColumnDescriptor;
use crate::sql::schema::StreamSchemaProvider;
use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::table::Table;
use crate::sql::types::TIMESTAMP_FIELD;

/// Rewrites table scans: projections are lifted out of scans into a dedicated projection node
/// (including virtual fields), using a connector table-source extension instead of a bare
/// `TableScan`, optionally with Debezium unrolling for updating sources, then remote boundary and
/// watermark.
pub struct SourceRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
}

impl<'a> SourceRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }
}

impl SourceRewriter<'_> {
    fn projection_expr_for_column(col: &ColumnDescriptor, qualifier: &TableReference) -> Expr {
        if let Some(logic) = col.computation_logic() {
            logic.clone().alias_qualified(
                Some(qualifier.clone()),
                col.arrow_field().name().to_string(),
            )
        } else {
            Expr::Column(Column {
                relation: Some(qualifier.clone()),
                name: col.arrow_field().name().to_string(),
                spans: Default::default(),
            })
        }
    }

    fn watermark_expression(table: &SourceTable) -> DFResult<Expr> {
        match table.temporal_config.watermark_strategy_column.clone() {
            Some(watermark_field) => table
                .schema_specs
                .iter()
                .find_map(|c| {
                    if c.arrow_field().name() == watermark_field.as_str() {
                        return if let Some(expr) = c.computation_logic() {
                            Some(expr.clone())
                        } else {
                            Some(Expr::Column(Column {
                                relation: None,
                                name: c.arrow_field().name().to_string(),
                                spans: Default::default(),
                            }))
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
        table: &SourceTable,
        qualifier: &TableReference,
        projection: &Option<Vec<usize>>,
    ) -> DFResult<Vec<Expr>> {
        let mut expressions: Vec<Expr> = table
            .schema_specs
            .iter()
            .map(|col| Self::projection_expr_for_column(col, qualifier))
            .collect();

        if let Some(proj) = projection {
            expressions = proj.iter().map(|i| expressions[*i].clone()).collect();
        }

        if let Some(event_time_field) = table.temporal_config.event_column.clone() {
            let expr = table
                .schema_specs
                .iter()
                .find_map(|c| {
                    if c.arrow_field().name() == event_time_field.as_str() {
                        return Some(Self::projection_expr_for_column(c, qualifier));
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Event time field {event_time_field} not found"))
                })?;

            expressions
                .push(expr.alias_qualified(Some(qualifier.clone()), TIMESTAMP_FIELD.to_string()));
        } else {
            let has_ts = table
                .schema_specs
                .iter()
                .any(|c| c.arrow_field().name() == TIMESTAMP_FIELD);
            if !has_ts {
                return plan_err!(
                    "Connector table '{}' has no `{}` column; declare WATERMARK FOR <event_time> AS ... in CREATE TABLE",
                    table.table_identifier,
                    TIMESTAMP_FIELD
                );
            }
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                TIMESTAMP_FIELD,
            )));
        }

        if table.is_updating() {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                UPDATING_META_FIELD,
            )));
        }

        Ok(expressions)
    }

    /// Connector path: `StreamIngestionNode` (table source) → optional `UnrollDebeziumPayloadNode`
    /// → `Projection`, mirroring Arroyo `TableSourceExtension` + Debezium unroll + projection.
    fn projection(&self, table_scan: &TableScan, table: &SourceTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        let table_source = LogicalPlan::Extension(Extension {
            node: Arc::new(StreamIngestionNode::try_new(
                qualifier.clone(),
                table.clone(),
            )?),
        });

        let (projection_input, scan_projection) = if table.is_updating() {
            if table.key_constraints.is_empty() {
                return plan_err!(
                    "Updating connector table `{}` requires at least one PRIMARY KEY for CDC unrolling",
                    table.table_identifier
                );
            }
            let unrolled = LogicalPlan::Extension(Extension {
                node: Arc::new(UnrollDebeziumPayloadNode::try_new(
                    table_source,
                    Arc::new(table.key_constraints.clone()),
                )?),
            });
            (unrolled, None)
        } else {
            (table_source, table_scan.projection.clone())
        };

        Ok(LogicalPlan::Projection(Projection::try_new(
            Self::projection_expressions(table, &qualifier, &scan_projection)?,
            Arc::new(projection_input),
        )?))
    }

    fn mutate_connector_table(
        &self,
        table_scan: &TableScan,
        table: &SourceTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let input = self.projection(table_scan, table)?;

        let schema = input.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableBoundaryNode {
                upstream_plan: input,
                table_identifier: table_scan.table_name.to_owned(),
                resolved_schema: schema,
                requires_materialization: true,
            }),
        });

        let watermark_node = EventTimeWatermarkNode::try_new(
            remote,
            table_scan.table_name.clone(),
            Self::watermark_expression(table)?,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!("failed to create watermark node: {err}"))
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
