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
use crate::sql::logical_planner::optimizers::ChainingOptimizer;
use crate::sql::schema::Table;
use crate::sql::schema::connector::ConnectionType;
use crate::sql::schema::connector_table::ConnectorTable;
use crate::sql::schema::field_spec::FieldSpec;
use crate::sql::schema::optimizer::produce_optimized_plan;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::extensions::sink::SinkExtension;
use crate::sql::logical_planner::planner;
use crate::sql::analysis::{StreamSchemaProvider, maybe_add_key_extension_to_sink, rewrite_sinks};
use crate::sql::rewrite_plan;

const CONNECTOR: &str = "connector";
const PARTITION_BY: &str = "partition_by";
const IDLE_MICROS: &str = "idle_time";

/// Convert `WITH` option list to a key-value map (e.g. connector settings).
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
    /// Builds the logical plan for 'CREATE STREAMING TABLE'.
    /// This orchestrates the transformation from a SQL Query to a stateful Sink.
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

        // 1. Connector Options Extraction
        // Extract 'connector' (Kafka, Postgres, etc.) and other physical properties.
        let mut opts = ConnectorOptions::new(with_options, &None)?;
        let connector = opts.pull_opt_str(CONNECTOR)?.ok_or_else(|| {
            plan_datafusion_err!(
                "Streaming Table '{}' must specify the '{}' option",
                table_name,
                CONNECTOR
            )
        })?;

        // 2. Query Optimization & Streaming Rewrite
        // Convert the standard SQL query into a streaming-aware logical plan.
        let base_plan =
            produce_optimized_plan(&Statement::Query(query.clone()), &self.schema_provider)?;
        let mut plan = rewrite_plan(base_plan, &self.schema_provider)?;

        // 3. Outgoing Data Serialization
        // If the query produces internal types (like JSON Union), inject a serialization layer.
        if plan
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            plan = serialize_outgoing_json(&self.schema_provider, Arc::new(plan));
        }

        // 4. Sink Metadata & Partitioning Logic
        // Determine how data should be partitioned before hitting the external system.
        let partition_exprs = self.resolve_partition_expressions(&mut opts)?;

        // Map DataFusion fields to Arroyo FieldSpecs for the connector.
        let fields: Vec<FieldSpec> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| FieldSpec::Struct((**f).clone()))
            .collect();

        // 5. Connector Table Construction
        // This object acts as the 'Identity Card' for the Sink in the physical cluster.
        let connector_table = ConnectorTable {
            id: None,
            connector,
            name: table_name.clone(),
            connection_type: ConnectionType::Sink,
            fields,
            config: "".to_string(), // Filled by the coordinator later
            description: comment.clone().unwrap_or_default(),
            event_time_field: None,
            watermark_field: None,
            idle_time: opts.pull_opt_duration(IDLE_MICROS)?,
            primary_keys: Arc::new(vec![]), // PKs are inferred or explicitly set here
            inferred_fields: None,
            partition_exprs: Arc::new(partition_exprs),
            lookup_cache_ttl:None,
            lookup_cache_max_bytes:None,
        };

        // 6. Sink Extension & Final Rewrites
        // Wrap the plan in a SinkExtension and ensure Key/Partition alignment.
        let sink_extension = SinkExtension::new(
            TableReference::bare(table_name.clone()),
            Table::ConnectorTable(connector_table.clone()),
            plan.schema().clone(),
            Arc::new(plan),
        )?;

        // Ensure the data distribution matches the Sink's requirements (e.g., Shuffle by Partition Key)
        let plan_with_keys = maybe_add_key_extension_to_sink(LogicalPlan::Extension(Extension {
            node: Arc::new(sink_extension),
        }))?;

        // Global pass to wire inputs and handle shared sub-plans
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
            connector_table,
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
