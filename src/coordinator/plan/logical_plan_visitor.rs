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

use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::sql::sqlparser::ast::{SqlOption, Statement as DFStatement};
use datafusion_common::TableReference;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{Expr, Extension, LogicalPlan, col};
use sqlparser::ast::Statement;
use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, DropFunctionPlan, PlanNode,
    ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan, StreamingTable,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, ShowFunctions, StartFunction,
    StatementVisitor, StatementVisitorContext, StatementVisitorResult, StopFunction,
    StreamingTableStatement,
};
use crate::coordinator::tool::ConnectorOptions;
use crate::sql::logical_node::logical::{LogicalProgram, ProgramConfig};
use crate::sql::logical_planner::optimizers::{ChainingOptimizer, produce_optimized_plan};
use crate::sql::schema::Table;
use crate::sql::schema::ConnectionType;
use crate::sql::schema::source_table::SourceTable;
use crate::sql::schema::ColumnDescriptor;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::extensions::sink::StreamEgressNode;
use crate::sql::logical_planner::planner;
use crate::sql::analysis::{StreamSchemaProvider, maybe_add_key_extension_to_sink, rewrite_sinks};
use crate::sql::rewrite_plan;

const CONNECTOR: &str = "connector";
const PARTITION_BY: &str = "partition_by";

fn with_options_to_map(options: &[SqlOption]) -> std::collections::HashMap<String, String> {
    options
        .iter()
        .filter_map(|opt| match opt {
            SqlOption::KeyValue { key, value } => Some((
                key.value.clone(),
                value.to_string().trim_matches('\'').to_string(),
            )),
            _ => None,
        })
        .collect()
}

pub struct LogicalPlanVisitor {
    schema_provider: StreamSchemaProvider,
}

impl LogicalPlanVisitor {
    pub fn new(schema_provider: StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn visit(&self, analysis: &Analysis) -> Box<dyn PlanNode> {
        let context = StatementVisitorContext::Empty;
        let stmt = analysis.statement();

        let result = stmt.accept(self, &context);

        match result {
            StatementVisitorResult::Plan(plan) => plan,
            _ => panic!("LogicalPlanVisitor should return Plan"),
        }
    }
    fn build_create_streaming_table_plan(
        &self,
        stmt: &StreamingTableStatement,
    ) -> Result<Box<dyn PlanNode>> {
        let DFStatement::CreateStreamingTable {
            name,
            with_options,
            comment,
            query,
        } = &stmt.statement
        else {
            return plan_err!("Only CREATE STREAMING TABLE is supported in this context");
        };

        let table_name = name.to_string();
        debug!("Compiling Streaming Table Sink for: {}", table_name);

        let mut opts = ConnectorOptions::new(with_options, &None)?;
        let connector = opts.pull_opt_str(CONNECTOR)?.ok_or_else(|| {
            plan_datafusion_err!(
                "Streaming Table '{}' must specify the '{}' option",
                table_name,
                CONNECTOR
            )
        })?;

        let partition_exprs = self.resolve_partition_expressions(&mut opts)?;

        let base_plan =
            produce_optimized_plan(&Statement::Query(query.clone()), &self.schema_provider)?;
        let mut plan = rewrite_plan(base_plan, &self.schema_provider)?;

        if plan
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            plan = serialize_outgoing_json(&self.schema_provider, Arc::new(plan));
        }

        let fields: Vec<ColumnDescriptor> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| ColumnDescriptor::from((**f).clone()))
            .collect();

        let mut source_table = SourceTable::from_options(
            &table_name,
            &connector,
            false,
            fields,
            vec![],
            None,
            &mut opts,
            None,
            &self.schema_provider,
            Some(ConnectionType::Sink),
            comment.clone().unwrap_or_default(),
        )?;
        source_table.partition_exprs = Arc::new(partition_exprs);

        let sink_extension = StreamEgressNode::try_new(
            TableReference::bare(table_name.clone()),
            Table::ConnectorTable(source_table.clone()),
            plan.schema().clone(),
            plan,
        )?;

        let plan_with_keys = maybe_add_key_extension_to_sink(LogicalPlan::Extension(Extension {
            node: Arc::new(sink_extension),
        }))?;

        let final_extensions = rewrite_sinks(vec![plan_with_keys])?;
        let final_plan = final_extensions.into_iter().next().unwrap();



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

        let mut plan_to_graph_visitor =
            planner::PlanToGraphVisitor::new(&self.schema_provider, &session_state);

        plan_to_graph_visitor.add_plan(final_plan.clone())?;

        let graph = plan_to_graph_visitor.into_graph();

        let mut program = LogicalProgram::new(graph, ProgramConfig::default());

        program.optimize(&ChainingOptimizer {});


        Ok(Box::new(StreamingTable {
            name: table_name,
            comment: comment.clone(),
            source_table,
            logical_plan: final_plan,
        }))
    }

    fn resolve_partition_expressions(
        &self,
        opts: &mut ConnectorOptions,
    ) -> Result<Option<Vec<Expr>>> {
        opts.pull_opt_str(PARTITION_BY)?
            .map(|cols| {
                cols.split(',')
                    .map(|c| col(c.trim()))
                    .collect::<Vec<Expr>>()
            })
            .map(Ok)
            .transpose()
    }
}

impl StatementVisitor for LogicalPlanVisitor {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let function_source = stmt.get_function_source().clone();
        let config_source = stmt.get_config_source().cloned();
        let extra_props = stmt.get_extra_properties().clone();

        StatementVisitorResult::Plan(Box::new(CreateFunctionPlan::new(
            function_source,
            config_source,
            extra_props,
        )))
    }

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StartFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StopFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_show_functions(
        &self,
        _stmt: &ShowFunctions,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowFunctionsPlan::new()))
    }

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let class_name = stmt.get_class_name().to_string();
        let modules = stmt.get_modules().to_vec();
        let config_content = stmt.get_config_content().to_string();

        StatementVisitorResult::Plan(Box::new(CreatePythonFunctionPlan::new(
            class_name,
            modules,
            config_content,
        )))
    }

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let sql_to_rel = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);

        match sql_to_rel.sql_statement_to_plan(stmt.statement.clone()) {
            Ok(plan) => {
                debug!("Create table plan:\n{}", plan.display_graphviz());
                StatementVisitorResult::Plan(Box::new(CreateTablePlan::new(plan)))
            }
            Err(e) => {
                panic!("Failed to convert CREATE TABLE to logical plan: {e}");
            }
        }
    }

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        match self.build_create_streaming_table_plan(stmt) {
            Ok(plan) => StatementVisitorResult::Plan(plan),
            Err(e) => panic!("Failed to build CreateStreamingTable plan: {e}"),
        }
    }
}

#[cfg(test)]
mod create_streaming_table_tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::sql::sqlparser::ast::Statement as DFStatement;
    use datafusion::sql::sqlparser::dialect::FunctionStreamDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use crate::sql::common::TIMESTAMP_FIELD;
    use crate::sql::rewrite_plan;
    use crate::sql::logical_planner::optimizers::produce_optimized_plan;
    use crate::sql::schema::StreamSchemaProvider;

    fn schema_provider_with_src() -> StreamSchemaProvider {
        let mut provider = StreamSchemaProvider::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        provider.add_source_table(
            "src".to_string(),
            schema,
            Some(TIMESTAMP_FIELD.to_string()),
            None,
        );
        provider
    }

    #[test]
    fn create_streaming_table_query_plans_and_rewrites() {
        let sql =
            "CREATE STREAMING TABLE my_sink WITH ('connector' = 'kafka') AS SELECT * FROM src";
        let dialect = FunctionStreamDialect {};
        let ast = Parser::parse_sql(&dialect, sql).expect("parse CREATE STREAMING TABLE");
        let DFStatement::CreateStreamingTable { query, .. } = &ast[0] else {
            panic!("expected CreateStreamingTable, got {:?}", ast[0]);
        };
        let provider = schema_provider_with_src();
        let base = produce_optimized_plan(&DFStatement::Query(query.clone()), &provider)
            .expect("produce optimized logical plan for sink query");
        let rewritten = rewrite_plan(base, &provider).expect("streaming rewrite_plan");
        let dot = format!("{}", rewritten.display_graphviz());
        assert!(
            dot.contains("src") || dot.contains("Src"),
            "rewritten plan should reference source; got subgraph:\n{dot}"
        );
    }
}
