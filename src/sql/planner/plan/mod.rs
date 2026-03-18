use datafusion::common::Result;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::LogicalPlan;

use crate::sql::planner::StreamSchemaProvider;
use crate::sql::planner::rewrite::TimeWindowUdfChecker;

use self::aggregate::AggregateRewriter;
use self::join::JoinRewriter;
use self::stream_rewriter::StreamRewriter;
use self::window_detecting_visitor::{WindowDetectingVisitor, extract_column};
use self::window_fn::WindowFunctionRewriter;

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod stream_rewriter;
pub(crate) mod window_detecting_visitor;
pub(crate) mod window_fn;

use tracing::debug;

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
