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

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::OptimizerContext;
use datafusion::optimizer::OptimizerRule;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_lateral_join::DecorrelateLateralJoin;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_nested_union::EliminateNestedUnion;
use datafusion::optimizer::eliminate_one_union::EliminateOneUnion;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::Statement;

use crate::sql::schema::StreamSchemaProvider;

/// Converts a SQL statement into an optimized DataFusion logical plan.
///
/// Applies the DataFusion analyzer followed by a curated set of optimizer rules
/// suitable for streaming SQL (some rules like OptimizeProjections are excluded
/// because they can drop event-time calculation fields).
pub fn produce_optimized_plan(
    statement: &Statement,
    schema_provider: &StreamSchemaProvider,
) -> Result<LogicalPlan> {
    let sql_to_rel = SqlToRel::new(schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

    let analyzed_plan = schema_provider.analyzer.execute_and_check(
        plan,
        &ConfigOptions::default(),
        |_plan, _rule| {},
    )?;

    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(EliminateNestedUnion::new()),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(DecorrelateLateralJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        Arc::new(EliminateOneUnion::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        Arc::new(PushDownLimit::new()),
        Arc::new(PushDownFilter::new()),
        Arc::new(EliminateGroupByConstant::new()),
        Arc::new(CommonSubexprEliminate::new()),
    ];

    let optimizer = Optimizer::with_rules(rules);
    let optimized = optimizer.optimize(
        analyzed_plan,
        &OptimizerContext::default(),
        |_plan, _rule| {},
    )?;

    Ok(optimized)
}
