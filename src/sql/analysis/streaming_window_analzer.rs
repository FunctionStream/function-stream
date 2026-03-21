use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, DFSchema, DataFusionError, Result};
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan, expr::Alias};

use crate::sql::extensions::aggregate::{AGGREGATE_EXTENSION_NAME, AggregateExtension};
use crate::sql::extensions::join::JOIN_NODE_NAME;
use crate::sql::types::{DFField, WindowBehavior, WindowType, fields_with_qualifiers, find_window};

/// WindowDetectingVisitor identifies windowing strategies and tracks window-carrying fields
/// as they propagate upward through the logical plan tree.
#[derive(Debug, Default)]
pub(crate) struct StreamingWindowAnalzer {
    /// The specific window type discovered (Tumble, Hop, etc.)
    pub(crate) window: Option<WindowType>,
    /// Set of fields in the current plan node that carry window semantics.
    pub(crate) fields: HashSet<DFField>,
}

impl StreamingWindowAnalzer {
    /// Entry point to resolve the WindowType of a given plan branch.
    pub(crate) fn get_window(logical_plan: &LogicalPlan) -> Result<Option<WindowType>> {
        let mut visitor = Self::default();
        logical_plan.visit_with_subqueries(&mut visitor)?;
        Ok(visitor.window)
    }

    /// Resolves whether an expression is a reference to an existing window field
    /// or a definition of a new window function.
    fn resolve_window_from_expr(
        &self,
        expr: &Expr,
        input_schema: &DFSchema,
    ) -> Result<Option<WindowType>> {
        // 1. Check if the expression directly references a known window field.
        if let Some(col) = extract_column(expr) {
            let field = input_schema.field_with_name(col.relation.as_ref(), &col.name)?;
            let df_field: DFField = (col.relation.clone(), Arc::new(field.clone())).into();

            if self.fields.contains(&df_field) {
                return Ok(self.window.clone());
            }
        }

        // 2. Otherwise, check if it's a new window function call (e.g., tumble(), hop()).
        find_window(expr)
    }

    /// Updates the internal state with new window findings and maps them to the output schema.
    fn update_state(
        &mut self,
        matched_windows: Vec<(usize, WindowType)>,
        schema: &DFSchema,
    ) -> Result<()> {
        // Clear fields from the previous level to maintain schema strictly for the current node.
        self.fields.clear();

        for (index, window) in matched_windows {
            if let Some(existing) = &self.window {
                if existing != &window {
                    return Err(DataFusionError::Plan(format!(
                        "Conflicting windows in the same operator: expected {:?}, found {:?}",
                        existing, window
                    )));
                }
            } else {
                self.window = Some(window);
            }
            // Record this specific index in the schema as a window carrier.
            self.fields.insert(schema.qualified_field(index).into());
        }
        Ok(())
    }
}

pub(crate) fn extract_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(Alias { expr, .. }) => extract_column(expr),
        _ => None,
    }
}

impl TreeNodeVisitor<'_> for StreamingWindowAnalzer {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        // Joins require cross-branch validation to ensure left and right sides align on time.
        if let LogicalPlan::Extension(Extension { node }) = node
            && node.name() == JOIN_NODE_NAME
        {
            let mut branch_windows = HashSet::new();
            for input in node.inputs() {
                if let Some(w) = Self::get_window(input)? {
                    branch_windows.insert(w);
                }
            }

            if branch_windows.len() > 1 {
                return Err(DataFusionError::Plan(
                    "Join inputs have mismatched windowing strategies.".into(),
                ));
            }
            self.window = branch_windows.into_iter().next();

            // Optimization: No need to recurse manually if we've resolved the join boundary.
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Projection(p) => {
                let windows = p
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(i, e)| {
                        self.resolve_window_from_expr(e, p.input.schema())
                            .transpose()
                            .map(|res| res.map(|w| (i, w)))
                    })
                    .collect::<Result<Vec<_>>>()?;

                self.update_state(windows, &p.schema)?;
            }

            LogicalPlan::Aggregate(agg) => {
                let windows = agg
                    .group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(i, e)| {
                        self.resolve_window_from_expr(e, agg.input.schema())
                            .transpose()
                            .map(|res| res.map(|w| (i, w)))
                    })
                    .collect::<Result<Vec<_>>>()?;

                self.update_state(windows, &agg.schema)?;
            }

            LogicalPlan::SubqueryAlias(sa) => {
                // Map fields through the alias layer by resolving column indices.
                let input_schema = sa.input.schema();
                let mapped = self
                    .fields
                    .drain()
                    .map(|f| {
                        let idx = input_schema.index_of_column(&f.qualified_column())?;
                        Ok(sa.schema.qualified_field(idx).into())
                    })
                    .collect::<Result<HashSet<_>>>()?;

                self.fields = mapped;
            }

            LogicalPlan::Extension(Extension { node })
                if node.name() == AGGREGATE_EXTENSION_NAME =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<AggregateExtension>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("AggregateExtension node is malformed".into())
                    })?;

                match &ext.window_behavior {
                    WindowBehavior::FromOperator {
                        window,
                        window_field,
                        is_nested,
                        ..
                    } => {
                        if self.window.is_some() && !*is_nested {
                            return Err(DataFusionError::Plan(
                                "Redundant window definition on an already windowed stream.".into(),
                            ));
                        }
                        self.window = Some(window.clone());
                        self.fields.insert(window_field.clone());
                    }
                    WindowBehavior::InData => {
                        let current_schema_fields: HashSet<_> =
                            fields_with_qualifiers(node.schema()).into_iter().collect();
                        self.fields.retain(|f| current_schema_fields.contains(f));

                        if self.fields.is_empty() {
                            return Err(DataFusionError::Plan(
                                "Windowed aggregate missing window metadata from its input.".into(),
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
