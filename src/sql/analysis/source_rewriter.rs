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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, DataFusionError, Result as DFResult, TableReference, plan_err};
use datafusion::logical_expr::{
    self, BinaryExpr, Expr, Extension, LogicalPlan, Projection, TableScan,
};

use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::ColumnDescriptor;
use crate::sql::schema::table::Table;
use crate::sql::schema::StreamSchemaProvider;
use crate::sql::schema::StreamTable;
use crate::sql::common::constants::sql_field;
use crate::sql::extensions::remote_table::RemoteTableBoundaryNode;
use crate::sql::extensions::watermark_node::EventTimeWatermarkNode;
use crate::sql::types::TIMESTAMP_FIELD;

/// Rewrites table scans into proper source nodes with projections and watermarks.
pub struct SourceRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
}

impl<'a> SourceRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }
}

impl SourceRewriter<'_> {
    /// Output column names after stream-catalog source projection (physical fields plus optional
    /// `_timestamp` alias when event time is renamed).
    fn stream_source_projected_column_names(
        schema: &datafusion::arrow::datatypes::Schema,
        event_time_field: Option<&str>,
    ) -> HashSet<String> {
        let mut names: HashSet<String> =
            schema.fields().iter().map(|f| f.name().clone()).collect();
        if let Some(et) = event_time_field {
            if et != TIMESTAMP_FIELD {
                names.insert(TIMESTAMP_FIELD.to_string());
            }
        }
        names
    }

    /// Resolves watermark column for [`StreamTable::Source`]: drop computed `__watermark` and any
    /// name not present in the projected schema (defaults to `_timestamp` − delay).
    fn stream_source_effective_watermark_field<'b>(
        watermark_field: Option<&'b str>,
        projected: &HashSet<String>,
    ) -> Option<&'b str> {
        let w = watermark_field?;
        if w == sql_field::COMPUTED_WATERMARK {
            return None;
        }
        projected.contains(w).then_some(w)
    }

    fn projection_expr_for_column(col: &ColumnDescriptor, qualifier: &TableReference) -> Expr {
        if let Some(logic) = col.computation_logic() {
            logic
                .clone()
                .alias_qualified(Some(qualifier.clone()), col.arrow_field().name().to_string())
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

        Ok(expressions)
    }

    /// Stream catalog [`StreamTable::Source`] (Kafka/… registered via coordinator): inject `_timestamp`
    /// from `event_time_field` when the physical schema uses another name (e.g. `impression_time`).
    fn mutate_stream_catalog_source(
        &self,
        table_scan: &TableScan,
        st: &StreamTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let StreamTable::Source {
            schema,
            event_time_field,
            watermark_field,
            ..
        } = st
        else {
            return Ok(Transformed::no(LogicalPlan::TableScan(table_scan.clone())));
        };

        let qualifier = table_scan.table_name.clone();

        let mut expressions: Vec<Expr> = schema
            .fields()
            .iter()
            .map(|f| {
                Expr::Column(Column {
                    relation: Some(qualifier.clone()),
                    name: f.name().to_string(),
                    spans: Default::default(),
                })
            })
            .collect();

        let has_physical_ts = schema.fields().iter().any(|f| f.name() == TIMESTAMP_FIELD);

        match event_time_field.as_deref() {
            Some(et) if et != TIMESTAMP_FIELD => {
                if !schema.fields().iter().any(|f| f.name().as_str() == et) {
                    return Err(DataFusionError::Plan(format!(
                        "Stream source `{}`: event_time_field `{et}` is not in the table schema",
                        table_scan.table_name.table()
                    )));
                }
                expressions.push(
                    Expr::Column(Column {
                        relation: Some(qualifier.clone()),
                        name: et.to_string(),
                        spans: Default::default(),
                    })
                    .alias_qualified(Some(qualifier.clone()), TIMESTAMP_FIELD.to_string()),
                );
            }
            None if !has_physical_ts => {
                return plan_err!(
                    "Stream source `{}` has no `{}` column; declare WATERMARK FOR <event_time> AS ... on CREATE TABLE, or add a `{}` column",
                    table_scan.table_name.table(),
                    TIMESTAMP_FIELD,
                    TIMESTAMP_FIELD
                );
            }
            _ => {}
        }

        let source_input = LogicalPlan::TableScan(table_scan.clone());
        let projection = LogicalPlan::Projection(Projection::try_new(
            expressions,
            Arc::new(source_input),
        )?);

        let schema_ref = projection.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableBoundaryNode {
                upstream_plan: projection,
                table_identifier: table_scan.table_name.to_owned(),
                resolved_schema: schema_ref,
                requires_materialization: true,
            }),
        });

        let projected = Self::stream_source_projected_column_names(
            schema.as_ref(),
            event_time_field.as_deref(),
        );
        let wf = Self::stream_source_effective_watermark_field(
            watermark_field.as_deref(),
            &projected,
        );
        let wm_expr = Self::watermark_expression_for_stream_source(wf, &qualifier)?;

        let watermark_node = EventTimeWatermarkNode::try_new(
            remote,
            table_scan.table_name.clone(),
            wm_expr,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!("failed to create watermark node: {err}"))
        })?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        })))
    }

    fn watermark_expression_for_stream_source(
        watermark_field: Option<&str>,
        qualifier: &TableReference,
    ) -> DFResult<Expr> {
        match watermark_field {
            Some(wf) => Ok(Expr::Column(Column {
                relation: Some(qualifier.clone()),
                name: wf.to_string(),
                spans: Default::default(),
            })),
            None => Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: Some(qualifier.clone()),
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

    fn projection(&self, table_scan: &TableScan, table: &SourceTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        // TODO: replace with StreamIngestionNode when available
        let source_input = LogicalPlan::TableScan(table_scan.clone());

        Ok(LogicalPlan::Projection(Projection::try_new(
            Self::projection_expressions(table, &qualifier, &table_scan.projection)?,
            Arc::new(source_input),
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

        if let Some(table) = self.schema_provider.get_catalog_table(table_name) {
            return match table {
                Table::ConnectorTable(table) => self.mutate_connector_table(&table_scan, table),
                Table::LookupTable(_table) => {
                    // TODO: implement LookupSource extension
                    plan_err!("Lookup tables are not yet supported")
                }
                Table::TableFromQuery {
                    name: _,
                    logical_plan,
                } => self.mutate_table_from_query(&table_scan, logical_plan),
            };
        }

        if let Some(st) = self.schema_provider.get_stream_table(table_name) {
            return self.mutate_stream_catalog_source(&table_scan, st.as_ref());
        }

        Ok(Transformed::no(LogicalPlan::TableScan(table_scan.clone())))
    }
}
