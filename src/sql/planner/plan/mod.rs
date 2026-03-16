use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{
    Column, DataFusionError, Result, Spans, TableReference, plan_err,
    tree_node::{TreeNode, TreeNodeRewriter, TreeNodeVisitor},
};
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, Filter, LogicalPlan, SubqueryAlias, expr::Alias,
};

use crate::sql::planner::extension::StreamExtension;
use crate::sql::planner::extension::aggregate::{AGGREGATE_EXTENSION_NAME, AggregateExtension};
use crate::sql::planner::extension::join::JOIN_NODE_NAME;
use crate::sql::planner::extension::remote_table::RemoteTableExtension;
use crate::sql::planner::schemas::{add_timestamp_field, has_timestamp_field};
use crate::sql::types::{
    DFField, TIMESTAMP_FIELD, WindowBehavior, WindowType, fields_with_qualifiers, find_window,
};

use self::aggregate::AggregateRewriter;
use self::join::JoinRewriter;
use self::window_fn::WindowFunctionRewriter;

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod window_fn;

use super::StreamSchemaProvider;
use tracing::debug;

/// Stage 3: LogicalPlan → Streaming LogicalPlan
///
/// Rewrites a standard DataFusion logical plan into one that supports
/// streaming semantics (timestamps, windows, watermarks).
pub fn rewrite_plan(
    plan: LogicalPlan,
    schema_provider: &StreamSchemaProvider,
) -> Result<LogicalPlan> {
    let rewritten_plan = plan.rewrite_with_subqueries(&mut StreamRewriter { schema_provider })?;

    rewritten_plan
        .data
        .visit_with_subqueries(&mut TimeWindowUdfChecker {})?;

    debug!(
        "Streaming logical plan:\n{}",
        rewritten_plan.data.display_graphviz()
    );

    Ok(rewritten_plan.data)
}

/// Visitor that detects window types in a logical plan
#[derive(Debug, Default)]
pub(crate) struct WindowDetectingVisitor {
    pub(crate) window: Option<WindowType>,
    pub(crate) fields: HashSet<DFField>,
}

impl WindowDetectingVisitor {
    pub(crate) fn get_window(logical_plan: &LogicalPlan) -> Result<Option<WindowType>> {
        let mut visitor = WindowDetectingVisitor {
            window: None,
            fields: HashSet::new(),
        };
        logical_plan.visit_with_subqueries(&mut visitor)?;
        Ok(visitor.window.take())
    }
}

fn extract_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(Alias { expr, .. }) => extract_column(expr),
        _ => None,
    }
}

impl TreeNodeVisitor<'_> for WindowDetectingVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        if node.name() == JOIN_NODE_NAME {
            let input_windows: HashSet<_> = node
                .inputs()
                .iter()
                .map(|input| Self::get_window(input))
                .collect::<Result<HashSet<_>>>()?;
            if input_windows.len() > 1 {
                return Err(DataFusionError::Plan(
                    "can't handle mixed windowing between left and right".to_string(),
                ));
            }
            self.window = input_windows
                .into_iter()
                .next()
                .expect("join has at least one input");
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Projection(projection) => {
                let window_expressions = projection
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = projection
                                .input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => return Some(Err(err)),
                            };
                            if self.fields.contains(
                                &(column.relation.clone(), Arc::new(input_field.clone())).into(),
                            ) {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.fields.clear();
                for (index, window) in window_expressions {
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return plan_err!(
                                "can't window by both {:?} and {:?}",
                                existing_window,
                                window
                            );
                        }
                        self.fields
                            .insert(projection.schema.qualified_field(index).into());
                    } else {
                        return plan_err!(
                            "can't call a windowing function without grouping by it in an aggregate"
                        );
                    }
                }
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.fields = self
                    .fields
                    .drain()
                    .map(|field| {
                        Ok(subquery_alias
                            .schema
                            .qualified_field(
                                subquery_alias
                                    .input
                                    .schema()
                                    .index_of_column(&field.qualified_column())?,
                            )
                            .into())
                    })
                    .collect::<Result<HashSet<_>>>()?;
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr: _,
                schema,
                ..
            }) => {
                let window_expressions = group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => return Some(Err(err)),
                            };
                            if self
                                .fields
                                .contains(&(column.relation.as_ref(), input_field).into())
                            {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.fields.clear();
                for (index, window) in window_expressions {
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window);
                    }
                    self.fields.insert(schema.qualified_field(index).into());
                }
            }
            LogicalPlan::Extension(Extension { node }) => {
                if node.name() == AGGREGATE_EXTENSION_NAME {
                    let aggregate_extension = node
                        .as_any()
                        .downcast_ref::<AggregateExtension>()
                        .expect("should be aggregate extension");

                    match &aggregate_extension.window_behavior {
                        WindowBehavior::FromOperator {
                            window,
                            window_field,
                            window_index: _,
                            is_nested,
                        } => {
                            if self.window.is_some() && !*is_nested {
                                return Err(DataFusionError::Plan(
                                    "aggregate node should not be recalculating window, as input is windowed.".to_string(),
                                ));
                            }
                            self.window = Some(window.clone());
                            self.fields.insert(window_field.clone());
                        }
                        WindowBehavior::InData => {
                            let input_fields = self.fields.clone();
                            self.fields.clear();
                            for field in fields_with_qualifiers(node.schema()) {
                                if input_fields.contains(&field) {
                                    self.fields.insert(field);
                                }
                            }
                            if self.fields.is_empty() {
                                return Err(DataFusionError::Plan(
                                    "must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Main rewriter for streaming SQL plans.
/// Rewrites standard logical plans into streaming-aware plans with
/// timestamp propagation, window detection, and streaming operator insertion.
pub struct StreamRewriter<'a> {
    pub(crate) schema_provider: &'a StreamSchemaProvider,
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

                // Rewrite row_time() calls to _timestamp column references
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

/// Rewrites row_time() function calls to _timestamp column references
struct RowTimeRewriter;

impl TreeNodeRewriter for RowTimeRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Expr::ScalarFunction(ref func) = node {
            if func.func.name() == "row_time" {
                return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    TIMESTAMP_FIELD.to_string(),
                ))));
            }
        }
        Ok(Transformed::no(node))
    }
}

/// Removes IS NOT NULL checks on window expressions that get pushed down incorrectly
pub(crate) struct TimeWindowNullCheckRemover;

impl TreeNodeRewriter for TimeWindowNullCheckRemover {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Expr::IsNotNull(ref inner) = node {
            if find_window(inner)?.is_some() {
                return Ok(Transformed::yes(Expr::Literal(
                    datafusion::common::ScalarValue::Boolean(Some(true)),
                    None,
                )));
            }
        }
        Ok(Transformed::no(node))
    }
}

/// Checks that window UDFs (tumble/hop/session) are not used outside aggregates
pub(crate) struct TimeWindowUdfChecker;

impl TreeNodeVisitor<'_> for TimeWindowUdfChecker {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                if let Some(window) = find_window(expr)? {
                    return plan_err!(
                        "Window function {:?} can only be used as a GROUP BY expression in an aggregate",
                        window
                    );
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
