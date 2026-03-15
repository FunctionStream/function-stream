use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::Statement;
use tracing::debug;

use crate::sql::planner::StreamSchemaProvider;

/// Stage 2: Statement → LogicalPlan
///
/// Converts a parsed SQL AST statement into a DataFusion logical plan
/// using the StreamSchemaProvider as the catalog context.
pub fn statement_to_plan(
    statement: Statement,
    schema_provider: &StreamSchemaProvider,
) -> Result<LogicalPlan> {
    let sql_to_rel = datafusion::sql::planner::SqlToRel::new(schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement)?;

    debug!("Logical plan:\n{}", plan.display_graphviz());

    Ok(plan)
}
