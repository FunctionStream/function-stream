use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use tracing::{debug, info, instrument};

use crate::sql::planner::StreamSchemaProvider;
use crate::sql::planner::plan::stream_rewriter::StreamRewriter;
use crate::sql::planner::rewrite::TimeWindowUdfChecker;

// Module declarations
pub(crate) mod aggregate_rewriter;
pub(crate) mod join_rewriter;
pub(crate) mod row_time_rewriter;
pub(crate) mod stream_rewriter;
pub(crate) mod streaming_window_analzer;
pub(crate) mod window_function_rewriter;

/// Entry point for transforming a standard DataFusion LogicalPlan into a
/// Streaming-aware LogicalPlan.
///
/// This function coordinates multiple rewriting passes and ensures the
/// resulting plan satisfies streaming constraints.
#[instrument(skip_all, level = "debug")]
pub fn rewrite_plan(
    plan: LogicalPlan,
    schema_provider: &StreamSchemaProvider,
) -> Result<LogicalPlan> {
    info!("Starting streaming plan rewrite pipeline");

    // Phase 1: Core Transformation
    // This pass handles the structural changes (Aggregates, Joins, Windows)
    // using a Bottom-Up traversal.
    let mut rewriter = StreamRewriter::new(schema_provider);
    let Transformed {
        data: rewritten_plan,
        ..
    } = plan.rewrite_with_subqueries(&mut rewriter)?;

    // Phase 2: Post-rewrite Validation
    // Ensure that the rewritten plan doesn't violate specific streaming UDF rules.
    rewritten_plan.visit_with_subqueries(&mut TimeWindowUdfChecker {})?;

    // Phase 3: Observability & Debugging
    // Industrial engines use Graphviz or specialized Explain formats for plan diffs.
    if cfg!(debug_assertions) {
        debug!(
            "Streaming logical plan graphviz:\n{}",
            rewritten_plan.display_graphviz()
        );
    }

    info!("Streaming plan rewrite completed successfully");
    Ok(rewritten_plan)
}
