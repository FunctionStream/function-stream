use std::sync::Arc;

use crate::sql::planner::extension::StreamExtension;
use crate::sql::planner::extension::remote_table::RemoteTableExtension;
use crate::sql::planner::plan::{
    aggregate::AggregateRewriter, join::JoinRewriter, window_fn::WindowFunctionRewriter,
};
use crate::sql::planner::rewrite::{RowTimeRewriter, TimeWindowNullCheckRemover};
use crate::sql::planner::schemas::{add_timestamp_field, has_timestamp_field};
use crate::sql::types::{DFField, TIMESTAMP_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, DataFusionError, Result, Spans, TableReference, plan_err};
use datafusion::logical_expr::{Expr, Extension, Filter, LogicalPlan, SubqueryAlias};
use datafusion_common::tree_node::TreeNode;

use super::StreamSchemaProvider;

pub struct StreamRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
}

impl<'a> StreamRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }
}

impl TreeNodeRewriter for StreamRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, mut node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::Projection(ref mut projection) => {
                if !has_timestamp_field(&projection.schema) {
                    let timestamp_field: DFField = projection
                        .input
                        .schema()
                        .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
                        .map_err(|_| {
                            DataFusionError::Plan(format!(
                                "No timestamp field found in projection input ({})",
                                projection.input.display()
                            ))
                        })?
                        .into();
                    projection.schema = add_timestamp_field(
                        projection.schema.clone(),
                        timestamp_field.qualifier().cloned(),
                    )
                    .expect("in projection");
                    projection.expr.push(Expr::Column(Column {
                        relation: timestamp_field.qualifier().cloned(),
                        name: TIMESTAMP_FIELD.to_string(),
                        spans: Spans::default(),
                    }));
                }

                let rewritten = projection
                    .expr
                    .iter()
                    .map(|expr| expr.clone().rewrite(&mut RowTimeRewriter {}))
                    .collect::<Result<Vec<_>>>()?;
                if rewritten.iter().any(|r| r.transformed) {
                    projection.expr = rewritten.into_iter().map(|r| r.data).collect();
                }
                return Ok(Transformed::yes(node));
            }
            LogicalPlan::Aggregate(aggregate) => {
                return AggregateRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Aggregate(aggregate));
            }
            LogicalPlan::Join(join) => {
                return JoinRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Join(join));
            }
            LogicalPlan::Filter(f) => {
                let expr = f
                    .predicate
                    .clone()
                    .rewrite(&mut TimeWindowNullCheckRemover {})?;
                return Ok(if expr.transformed {
                    Transformed::yes(LogicalPlan::Filter(Filter::try_new(expr.data, f.input)?))
                } else {
                    Transformed::no(LogicalPlan::Filter(f))
                });
            }
            LogicalPlan::Window(_) => {
                return WindowFunctionRewriter {}.f_up(node);
            }
            LogicalPlan::Sort(_) => {
                return plan_err!(
                    "ORDER BY is not currently supported in streaming SQL ({})",
                    node.display()
                );
            }
            LogicalPlan::Repartition(_) => {
                return plan_err!(
                    "Repartitions are not currently supported ({})",
                    node.display()
                );
            }
            LogicalPlan::Union(mut union) => {
                union.schema = union.inputs[0].schema().clone();
                for input in union.inputs.iter_mut() {
                    if let LogicalPlan::Extension(Extension { node }) = input.as_ref() {
                        let stream_extension: &dyn StreamExtension = node.try_into().unwrap();
                        if !stream_extension.transparent() {
                            continue;
                        }
                    }
                    let remote_table_extension = Arc::new(RemoteTableExtension {
                        input: input.as_ref().clone(),
                        name: TableReference::bare("union_input"),
                        schema: union.schema.clone(),
                        materialize: false,
                    });
                    *input = Arc::new(LogicalPlan::Extension(Extension {
                        node: remote_table_extension,
                    }));
                }
                return Ok(Transformed::yes(LogicalPlan::Union(union)));
            }
            LogicalPlan::SubqueryAlias(sa) => {
                return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(sa.input, sa.alias)?,
                )));
            }
            LogicalPlan::Limit(_) => {
                return plan_err!(
                    "LIMIT is not currently supported in streaming SQL ({})",
                    node.display()
                );
            }
            LogicalPlan::Explain(_) => {
                return plan_err!("EXPLAIN is not supported ({})", node.display());
            }
            LogicalPlan::Analyze(_) => {
                return plan_err!("ANALYZE is not supported ({})", node.display());
            }
            _ => {}
        }
        Ok(Transformed::no(node))
    }
}
