use std::sync::Arc;

use super::StreamSchemaProvider;
use crate::sql::planner::extension::StreamExtension;
use crate::sql::planner::extension::remote_table::RemoteTableExtension;
use crate::sql::planner::plan::row_time_rewriter::RowTimeRewriter;
use crate::sql::planner::plan::{
    aggregate_rewriter::AggregateRewriter, join_rewriter::JoinRewriter,
    window_function_rewriter::WindowFunctionRewriter,
};
use crate::sql::planner::rewrite::TimeWindowNullCheckRemover;
use crate::sql::planner::schemas::{add_timestamp_field, has_timestamp_field};
use crate::sql::types::{DFField, TIMESTAMP_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, DataFusionError, Result, Spans, TableReference, plan_err};
use datafusion::logical_expr::{
    Expr, Extension, Filter, LogicalPlan, Projection, SubqueryAlias, Union,
};
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::{Aggregate, Join};

pub struct StreamRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
}

impl TreeNodeRewriter for StreamRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            // Logic Delegation
            LogicalPlan::Projection(p) => self.rewrite_projection(p),
            LogicalPlan::Filter(f) => self.rewrite_filter(f),
            LogicalPlan::Union(u) => self.rewrite_union(u),

            // Delegation to specialized sub-rewriters
            LogicalPlan::Aggregate(agg) => self.rewrite_aggregate(agg),
            LogicalPlan::Join(join) => self.rewrite_join(join),
            LogicalPlan::Window(_) => self.rewrite_window(node),
            LogicalPlan::SubqueryAlias(sa) => self.rewrite_subquery_alias(sa),

            // Explicitly Unsupported Operations
            LogicalPlan::Sort(_) => self.unsupported_error("ORDER BY", &node),
            LogicalPlan::Limit(_) => self.unsupported_error("LIMIT", &node),
            LogicalPlan::Repartition(_) => self.unsupported_error("Repartitions", &node),
            LogicalPlan::Explain(_) => self.unsupported_error("EXPLAIN", &node),
            LogicalPlan::Analyze(_) => self.unsupported_error("ANALYZE", &node),

            _ => Ok(Transformed::no(node)),
        }
    }
}

impl<'a> StreamRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    /// Delegates to AggregateRewriter to transform batch aggregates into streaming stateful operators.
    fn rewrite_aggregate(&self, agg: Aggregate) -> Result<Transformed<LogicalPlan>> {
        AggregateRewriter {
            schema_provider: self.schema_provider,
        }
        .f_up(LogicalPlan::Aggregate(agg))
    }

    /// Delegates to JoinRewriter to handle streaming join semantics (e.g., TTL, state management).
    fn rewrite_join(&self, join: Join) -> Result<Transformed<LogicalPlan>> {
        JoinRewriter {
            schema_provider: self.schema_provider,
        }
        .f_up(LogicalPlan::Join(join))
    }

    /// Delegates to WindowFunctionRewriter for stream-aware windowing logic.
    fn rewrite_window(&self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        WindowFunctionRewriter {}.f_up(node)
    }

    /// Refreshes SubqueryAlias metadata to align with potentially rewritten internal schemas.
    fn rewrite_subquery_alias(&self, sa: SubqueryAlias) -> Result<Transformed<LogicalPlan>> {
        // Since the inner 'sa.input' has been rewritten (bottom-up), we must re-create
        // the alias node to ensure the outer schema correctly reflects internal changes.
        let new_sa = SubqueryAlias::try_new(sa.input, sa.alias).map_err(|e| {
            DataFusionError::Internal(format!("Failed to re-alias subquery: {}", e))
        })?;

        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(new_sa)))
    }

    /// Handles timestamp propagation and row_time() mapping for Projections
    fn rewrite_projection(&self, mut projection: Projection) -> Result<Transformed<LogicalPlan>> {
        // Check if the current projection already has a timestamp field;
        // if not, we must inject it to maintain streaming heartbeats.
        if !has_timestamp_field(&projection.schema) {
            let input_schema = projection.input.schema();

            // Resolve the timestamp field from the input schema using the global constant.
            let timestamp_field: DFField = input_schema
                .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
                .map_err(|_| {
                    DataFusionError::Plan(format!(
                        "No timestamp field found in projection input ({})",
                        projection.input.display()
                    ))
                })?
                .into();

            // Update the logical schema to include the newly injected timestamp.
            projection.schema = add_timestamp_field(
                projection.schema.clone(),
                timestamp_field.qualifier().cloned(),
            )
            .expect("Failed to add timestamp to projection schema");

            // Physically push the timestamp column into the expression list.
            projection.expr.push(Expr::Column(Column {
                relation: timestamp_field.qualifier().cloned(),
                name: TIMESTAMP_FIELD.to_string(),
                spans: Spans::default(),
            }));
        }

        // Map user-friendly row_time() function calls to internal _timestamp column references.
        let rewritten = projection
            .expr
            .iter()
            .map(|expr| expr.clone().rewrite(&mut RowTimeRewriter {}))
            .collect::<Result<Vec<_>>>()?;

        // If any expressions were modified (e.g., row_time() was replaced), update the projection.
        if rewritten.iter().any(|r| r.transformed) {
            projection.expr = rewritten.into_iter().map(|r| r.data).collect();
        }

        // Return the updated plan node wrapped in a Transformed container.
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }

    /// Harmonizes schemas across Union branches and wraps them in RemoteTableExtensions.
    ///
    /// This ensures that all inputs to a UNION operation share the exact same schema metadata,
    /// preventing "Schema Drift" where different branches have different field qualifiers.
    fn rewrite_union(&self, mut union: Union) -> Result<Transformed<LogicalPlan>> {
        // Industrial engines use the first branch as the "Master Schema" for the Union.
        // We clone it once to ensure all subsequent branches are forced to comply.
        let master_schema = union.inputs[0].schema().clone();
        union.schema = master_schema.clone();

        for input in union.inputs.iter_mut() {
            // Optimization: If the node is already a non-transparent Extension,
            // we skip wrapping to avoid unnecessary nesting of logical nodes.
            if let LogicalPlan::Extension(Extension { node }) = input.as_ref() {
                let stream_ext: &dyn StreamExtension = node.try_into().map_err(|e| {
                    DataFusionError::Internal(format!("Failed to resolve StreamExtension: {}", e))
                })?;

                if !stream_ext.transparent() {
                    continue;
                }
            }

            // Wrap each branch in a RemoteTableExtension.
            // This acts as a logical "bridge" that forces the input to adopt the master_schema,
            // effectively stripping away branch-specific qualifiers (e.g., table aliases).
            let remote_ext = Arc::new(RemoteTableExtension {
                input: input.as_ref().clone(),
                name: TableReference::bare("union_input"),
                schema: master_schema.clone(),
                materialize: false, // Internal logical boundary only; does not require physical sink.
            });

            // Atomically replace the input with the wrapped version.
            *input = Arc::new(LogicalPlan::Extension(Extension { node: remote_ext }));
        }

        Ok(Transformed::yes(LogicalPlan::Union(union)))
    }

    /// Optimizes Filter nodes by stripping redundant NULL checks on time window expressions.
    ///
    /// In streaming SQL, DataFusion often injects 'IS NOT NULL' guards for window functions
    /// that are redundant or can interfere with watermark propagation. This rewriter
    /// cleans those predicates to simplify the physical execution plan.
    fn rewrite_filter(&self, filter: Filter) -> Result<Transformed<LogicalPlan>> {
        // We attempt to rewrite the predicate using a specialized sub-rewriter.
        // The TimeWindowNullCheckRemover specifically targets expressions like
        // `tumble(...) IS NOT NULL` and simplifies them to `TRUE`.
        let rewritten_expr = filter
            .predicate
            .clone()
            .rewrite(&mut TimeWindowNullCheckRemover {})?;

        if !rewritten_expr.transformed {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        // Industrial Guard: Re-validate the predicate against the input schema.
        // 'Filter::try_new' ensures that the transformed expression is still semantically
        // valid for the underlying data stream.
        let new_filter = Filter::try_new(rewritten_expr.data, filter.input).map_err(|e| {
            DataFusionError::Internal(format!(
                "Failed to re-validate filtered predicate after NULL-check removal: {}",
                e
            ))
        })?;

        Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
    }

    /// Centralized error handler for unsupported streaming operations
    fn unsupported_error(&self, op: &str, node: &LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan_err!(
            "{} is not currently supported in streaming SQL ({})",
            op,
            node.display()
        )
    }
}
