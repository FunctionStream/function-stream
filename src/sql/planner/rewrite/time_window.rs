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

use datafusion::common::tree_node::{
    Transformed, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan};

/// Returns the time window function name if the expression is one (tumble/hop/session).
pub fn is_time_window(expr: &Expr) -> Option<&str> {
    if let Expr::ScalarFunction(ScalarFunction { func, args: _ }) = expr {
        match func.name() {
            "tumble" | "hop" | "session" => return Some(func.name()),
            _ => {}
        }
    }
    None
}

struct TimeWindowExprChecker {}

impl TreeNodeVisitor<'_> for TimeWindowExprChecker {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Some(w) = is_time_window(node) {
            return plan_err!(
                "time window function {} is not allowed in this context. \
                 Are you missing a GROUP BY clause?",
                w
            );
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Visitor that checks an entire LogicalPlan for misplaced time window UDFs.
pub struct TimeWindowUdfChecker {}

impl TreeNodeVisitor<'_> for TimeWindowUdfChecker {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        use datafusion::common::tree_node::TreeNode;
        node.expressions().iter().try_for_each(|expr| {
            let mut checker = TimeWindowExprChecker {};
            expr.visit(&mut checker)?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Removes `IS NOT NULL` checks wrapping time window functions,
/// replacing them with `true` since time windows are never null.
pub struct TimeWindowNullCheckRemover {}

impl TreeNodeRewriter for TimeWindowNullCheckRemover {
    type Node = Expr;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let Expr::IsNotNull(expr) = &node
            && is_time_window(expr).is_some()
        {
            return Ok(Transformed::yes(Expr::Literal(
                ScalarValue::Boolean(Some(true)),
                None,
            )));
        }
        Ok(Transformed::no(node))
    }
}
