#![allow(clippy::new_without_default)]

pub(crate) mod extension;
pub mod parse;
pub(crate) mod physical_planner;
pub mod plan;
pub mod rewrite;
pub mod schema_provider;
pub mod schemas;
pub mod sql_to_plan;

pub(crate) mod mod_prelude {
    pub use super::StreamSchemaProvider;
}

pub use schema_provider::{LogicalBatchInput, StreamSchemaProvider, StreamTable};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{OneOrManyWithParens, Statement};
use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
use datafusion::sql::sqlparser::parser::Parser;
use tracing::debug;

use crate::datastream::logical::{LogicalProgram, ProgramConfig};
use crate::datastream::optimizers::ChainingOptimizer;
use crate::sql::catalog::insert::Insert;
use crate::sql::catalog::table::Table as CatalogTable;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::planner::extension::key_calculation::{KeyCalculationExtension, KeysOrExprs};
use crate::sql::planner::extension::projection::ProjectionExtension;
use crate::sql::planner::extension::sink::SinkExtension;
use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::planner::plan::rewrite_plan;
use crate::sql::planner::rewrite::{SinkInputRewriter, SourceMetadataVisitor};
use crate::sql::types::SqlConfig;

// ── Compilation pipeline ──────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct CompiledSql {
    pub program: LogicalProgram,
    pub connection_ids: Vec<i64>,
}

pub fn parse_sql_statements(
    sql: &str,
) -> std::result::Result<Vec<Statement>, datafusion::sql::sqlparser::parser::ParserError> {
    Parser::parse_sql(&FunctionStreamDialect {}, sql)
}

fn try_handle_set_variable(
    statement: &Statement,
    schema_provider: &mut StreamSchemaProvider,
) -> Result<bool> {
    if let Statement::SetVariable {
        variables, value, ..
    } = statement
    {
        let OneOrManyWithParens::One(opt) = variables else {
            return plan_err!("invalid syntax for `SET` call");
        };

        if opt.to_string() != "updating_ttl" {
            return plan_err!(
                "invalid option '{}'; supported options are 'updating_ttl'",
                opt
            );
        }

        if value.len() != 1 {
            return plan_err!("invalid `SET updating_ttl` call; expected exactly one expression");
        }

        let duration = duration_from_sql_expr(&value[0])?;
        schema_provider.planning_options.ttl = duration;

        return Ok(true);
    }

    Ok(false)
}

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

pub async fn parse_and_get_arrow_program(
    query: String,
    mut schema_provider: StreamSchemaProvider,
    _config: SqlConfig,
) -> Result<CompiledSql> {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    config.options_mut().optimizer.repartition_aggregations = false;
    config.options_mut().optimizer.repartition_windows = false;
    config.options_mut().optimizer.repartition_sorts = false;
    config.options_mut().optimizer.repartition_joins = false;
    config.options_mut().execution.target_partitions = 1;

    let session_state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(vec![])
        .build();

    let mut inserts = vec![];
    for statement in parse_sql_statements(&query)? {
        if try_handle_set_variable(&statement, &mut schema_provider)? {
            continue;
        }

        if let Some(table) = CatalogTable::try_from_statement(&statement, &schema_provider)? {
            schema_provider.insert_catalog_table(table);
        } else {
            inserts.push(Insert::try_from_statement(&statement, &schema_provider)?);
        };
    }

    if inserts.is_empty() {
        return plan_err!("The provided SQL does not contain a query");
    }

    let mut used_connections = HashSet::new();
    let mut extensions = vec![];

    for insert in inserts {
        let (plan, sink_name) = match insert {
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => (logical_plan, Some(sink_name)),
            Insert::Anonymous { logical_plan } => (logical_plan, None),
        };

        let mut plan_rewrite = rewrite_plan(plan, &schema_provider)?;

        if plan_rewrite
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            plan_rewrite = serialize_outgoing_json(&schema_provider, Arc::new(plan_rewrite));
        }

        debug!("Plan = {}", plan_rewrite.display_graphviz());

        let mut metadata = SourceMetadataVisitor::new(&schema_provider);
        plan_rewrite.visit_with_subqueries(&mut metadata)?;
        used_connections.extend(metadata.connection_ids.iter());

        let sink = match sink_name {
            Some(sink_name) => {
                let table = schema_provider
                    .get_catalog_table_mut(&sink_name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Connection {sink_name} not found"))
                    })?;
                match table {
                    CatalogTable::ConnectorTable(c) => {
                        if let Some(id) = c.id {
                            used_connections.insert(id);
                        }

                        SinkExtension::new(
                            TableReference::bare(sink_name),
                            table.clone(),
                            plan_rewrite.schema().clone(),
                            Arc::new(plan_rewrite),
                        )
                    }
                    CatalogTable::LookupTable(_) => {
                        plan_err!("lookup (temporary) tables cannot be inserted into")
                    }
                    CatalogTable::TableFromQuery { .. } => {
                        plan_err!(
                            "shouldn't be inserting more data into a table made with CREATE TABLE AS"
                        )
                    }
                }
            }
            None => {
                return plan_err!(
                    "Anonymous query is not supported; use INSERT INTO <sink> SELECT ..."
                );
            }
        };
        extensions.push(LogicalPlan::Extension(Extension {
            node: Arc::new(sink?),
        }));
    }

    let extensions = rewrite_sinks(extensions)?;

    let mut plan_to_graph_visitor =
        physical_planner::PlanToGraphVisitor::new(&schema_provider, &session_state);
    for extension in extensions {
        plan_to_graph_visitor.add_plan(extension)?;
    }
    let graph = plan_to_graph_visitor.into_graph();

    let mut program = LogicalProgram::new(graph, ProgramConfig::default());

    program.optimize(&ChainingOptimizer {});

    Ok(CompiledSql {
        program,
        connection_ids: used_connections.into_iter().collect(),
    })
}
