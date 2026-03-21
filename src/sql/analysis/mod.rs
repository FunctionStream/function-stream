#![allow(clippy::new_without_default)]

pub(crate) mod aggregate_rewriter;
pub(crate) mod join_rewriter;
pub(crate) mod row_time_rewriter;
pub(crate) mod stream_rewriter;
pub(crate) mod streaming_window_analzer;
pub(crate) mod window_function_rewriter;

pub mod async_udf_rewriter;
pub mod sink_input_rewriter;
pub mod source_metadata_visitor;
pub mod source_rewriter;
pub mod time_window;
pub mod unnest_rewriter;

pub use async_udf_rewriter::{AsyncOptions, AsyncUdfRewriter};
pub use sink_input_rewriter::SinkInputRewriter;
pub use source_metadata_visitor::SourceMetadataVisitor;
pub use source_rewriter::SourceRewriter;
pub use time_window::{TimeWindowNullCheckRemover, TimeWindowUdfChecker, is_time_window};
pub use unnest_rewriter::{UNNESTED_COL, UnnestRewriter};

pub use crate::sql::schema::schema_provider::{
    LogicalBatchInput, StreamSchemaProvider, StreamTable,
};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{OneOrManyWithParens, Statement};
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;
use tracing::{debug, info, instrument};

use crate::sql::logical_planner::optimizers::ChainingOptimizer;
use crate::sql::schema::insert::Insert;
use crate::sql::schema::table::Table as CatalogTable;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::extensions::key_calculation::{KeyCalculationExtension, KeysOrExprs};
use crate::sql::extensions::projection::ProjectionExtension;
use crate::sql::extensions::sink::SinkExtension;
use crate::sql::extensions::{ StreamExtension};
use crate::sql::logical_planner::planner::NamedNode;
use crate::sql::types::SqlConfig;

fn duration_from_sql_expr(
    expr: &datafusion::sql::sqlparser::ast::Expr,
) -> Result<std::time::Duration> {
    use datafusion::sql::sqlparser::ast::Expr as SqlExpr;
    use datafusion::sql::sqlparser::ast::Value as SqlValue;
    use datafusion::sql::sqlparser::ast::ValueWithSpan;

    match expr {
        SqlExpr::Interval(interval) => {
            let value_str = match interval.value.as_ref() {
                SqlExpr::Value(ValueWithSpan {
                    value: SqlValue::SingleQuotedString(s),
                    ..
                }) => s.clone(),
                other => return plan_err!("expected interval string literal, found {other}"),
            };

            parse_interval_to_duration(&value_str)
        }
        SqlExpr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) => parse_interval_to_duration(s),
        other => plan_err!("expected an interval expression, found {other}"),
    }
}

fn parse_interval_to_duration(s: &str) -> Result<std::time::Duration> {
    let parts: Vec<&str> = s.trim().split_whitespace().collect();
    if parts.len() != 2 {
        return plan_err!("invalid interval string '{s}'; expected '<value> <unit>'");
    }
    let value: u64 = parts[0]
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("invalid interval number: {}", parts[0])))?;
    match parts[1].to_lowercase().as_str() {
        "second" | "seconds" | "s" => Ok(std::time::Duration::from_secs(value)),
        "minute" | "minutes" | "min" => Ok(std::time::Duration::from_secs(value * 60)),
        "hour" | "hours" | "h" => Ok(std::time::Duration::from_secs(value * 3600)),
        "day" | "days" | "d" => Ok(std::time::Duration::from_secs(value * 86400)),
        unit => plan_err!("unsupported interval unit '{unit}'"),
    }
}

fn build_sink_inputs(extensions: &[LogicalPlan]) -> HashMap<NamedNode, Vec<LogicalPlan>> {
    let mut sink_inputs = HashMap::<NamedNode, Vec<LogicalPlan>>::new();
    for extension in extensions.iter() {
        if let LogicalPlan::Extension(ext) = extension {
            if let Some(sink_node) = ext.node.as_any().downcast_ref::<SinkExtension>() {
                if let Some(named_node) = sink_node.node_name() {
                    let inputs = sink_node
                        .inputs()
                        .into_iter()
                        .cloned()
                        .collect::<Vec<LogicalPlan>>();
                    sink_inputs.entry(named_node).or_default().extend(inputs);
                }
            }
        }
    }
    sink_inputs
}

pub(crate) fn maybe_add_key_extension_to_sink(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Extension(ref ext) = plan else {
        return Ok(plan);
    };

    let Some(sink) = ext.node.as_any().downcast_ref::<SinkExtension>() else {
        return Ok(plan);
    };

    let Some(partition_exprs) = sink.table.partition_exprs() else {
        return Ok(plan);
    };

    if partition_exprs.is_empty() {
        return Ok(plan);
    }

    let inputs = plan
        .inputs()
        .into_iter()
        .map(|input| {
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(KeyCalculationExtension {
                    name: Some("key-calc-partition".to_string()),
                    schema: input.schema().clone(),
                    input: input.clone(),
                    keys: KeysOrExprs::Exprs(partition_exprs.clone()),
                }),
            }))
        })
        .collect::<Result<_>>()?;

    use datafusion::prelude::col;
    let unkey = LogicalPlan::Extension(Extension {
        node: Arc::new(
            ProjectionExtension::new(
                inputs,
                Some("unkey".to_string()),
                sink.schema().iter().map(|(_, f)| col(f.name())).collect(),
            )
            .shuffled(),
        ),
    });

    let node = sink.with_exprs_and_inputs(vec![], vec![unkey])?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

pub fn rewrite_sinks(extensions: Vec<LogicalPlan>) -> Result<Vec<LogicalPlan>> {
    let mut sink_inputs = build_sink_inputs(&extensions);
    let mut new_extensions = vec![];
    for extension in extensions {
        let mut rewriter = SinkInputRewriter::new(&mut sink_inputs);
        let result = extension.rewrite(&mut rewriter)?;
        if !rewriter.was_removed {
            new_extensions.push(result.data);
        }
    }

    new_extensions
        .into_iter()
        .map(maybe_add_key_extension_to_sink)
        .collect()

}

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

    let mut rewriter = stream_rewriter::StreamRewriter::new(schema_provider);
    let Transformed {
        data: rewritten_plan,
        ..
    } = plan.rewrite_with_subqueries(&mut rewriter)?;

    rewritten_plan.visit_with_subqueries(&mut TimeWindowUdfChecker {})?;

    if cfg!(debug_assertions) {
        debug!(
            "Streaming logical plan graphviz:\n{}",
            rewritten_plan.display_graphviz()
        );
    }

    info!("Streaming plan rewrite completed successfully");
    Ok(rewritten_plan)
}
