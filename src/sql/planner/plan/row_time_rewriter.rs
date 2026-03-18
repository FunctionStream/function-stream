use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, Result as DFResult};
use datafusion::logical_expr::Expr;

use crate::sql::types::TIMESTAMP_FIELD;

/// Replaces the virtual `row_time()` scalar function with a physical reference to `_timestamp`.
///
/// This is a critical mapping step that allows users to use a friendly SQL function
/// while the engine operates on the mandatory internal streaming timestamp.
pub struct RowTimeRewriter;

impl TreeNodeRewriter for RowTimeRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        // Use pattern matching to identify the 'row_time' scalar function.
        if let Expr::ScalarFunction(func) = &node
            && func.name() == "row_time"
        {
            // Map the virtual function to the physical internal timestamp column.
            // We use .alias() to preserve the original name "row_time()" in the output schema,
            // ensuring that user-facing column names do not change unexpectedly.
            let physical_col = Expr::Column(Column {
                relation: None,
                name: TIMESTAMP_FIELD.to_string(),
                spans: Default::default(),
            })
            .alias("row_time()");

            return Ok(Transformed::yes(physical_col));
        }

        Ok(Transformed::no(node))
    }
}
