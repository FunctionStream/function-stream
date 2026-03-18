use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::{
    Column, DataFusionError, Result,
    tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
};
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan, expr::Alias};

use crate::sql::planner::extension::aggregate::{AGGREGATE_EXTENSION_NAME, AggregateExtension};
use crate::sql::planner::extension::join::JOIN_NODE_NAME;
use crate::sql::types::{DFField, WindowBehavior, WindowType, fields_with_qualifiers, find_window};

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

pub(crate) fn extract_column(expr: &Expr) -> Option<&Column> {
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
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window);
                    }
                    self.fields
                        .insert(projection.schema.qualified_field(index).into());
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
