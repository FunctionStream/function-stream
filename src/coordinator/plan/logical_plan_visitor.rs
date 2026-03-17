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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, DropFunctionPlan,
    InsertStatementPlan, PlanNode, ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, InsertStatement,
    ShowFunctions, StartFunction, StatementVisitor, StatementVisitorContext,
    StatementVisitorResult, StopFunction,
};
use crate::datastream::logical::{LogicalProgram, ProgramConfig};
use crate::datastream::optimizers::ChainingOptimizer;
use crate::sql::catalog::insert::Insert;
use crate::sql::catalog::table::Table as CatalogTable;
use crate::sql::functions::{is_json_union, serialize_outgoing_json};
use crate::sql::planner::StreamSchemaProvider;
use crate::sql::planner::extension::sink::SinkExtension;
use crate::sql::planner::plan::rewrite_plan;
use crate::sql::planner::rewrite::SourceMetadataVisitor;
use crate::sql::planner::{physical_planner, rewrite_sinks};

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

    fn build_insert_plan(&self, stmt: &InsertStatement) -> Result<Box<dyn PlanNode>> {
        let insert = Insert::try_from_statement(&stmt.statement, &self.schema_provider)?;

        let (plan, sink_name) = match insert {
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => (logical_plan, Some(sink_name)),
            Insert::Anonymous { logical_plan } => (logical_plan, None),
        };

        let mut plan_rewrite = rewrite_plan(plan, &self.schema_provider)?;

        if plan_rewrite
            .schema()
            .fields()
            .iter()
            .any(|f| is_json_union(f.data_type()))
        {
            plan_rewrite = serialize_outgoing_json(&self.schema_provider, Arc::new(plan_rewrite));
        }

        debug!("Plan = {}", plan_rewrite.display_graphviz());

        let mut used_connections = HashSet::new();
        let mut metadata = SourceMetadataVisitor::new(&self.schema_provider);
        plan_rewrite.visit_with_subqueries(&mut metadata)?;
        used_connections.extend(metadata.connection_ids.iter());

        let sink = match sink_name {
            Some(sink_name) => {
                let table = self
                    .schema_provider
                    .get_catalog_table(&sink_name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Connection {sink_name} not found"))
                    })?;
                match &table {
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
                    CatalogTable::MemoryTable { .. } => {
                        return plan_err!(
                            "INSERT into memory tables is not supported in single-statement mode"
                        );
                    }
                    CatalogTable::LookupTable(_) => {
                        plan_err!("lookup (temporary) tables cannot be inserted into")
                    }
                    CatalogTable::TableFromQuery { .. } => {
                        plan_err!(
                            "shouldn't be inserting more data into a table made with CREATE TABLE AS"
                        )
                    }
                    CatalogTable::PreviewSink { .. } => {
                        plan_err!("queries shouldn't be able insert into preview sink.")
                    }
                }
            }
            None => SinkExtension::new(
                TableReference::parse_str("preview"),
                CatalogTable::PreviewSink {
                    logical_plan: plan_rewrite.clone(),
                },
                plan_rewrite.schema().clone(),
                Arc::new(plan_rewrite),
            ),
        };

        let extension = LogicalPlan::Extension(Extension {
            node: Arc::new(sink?),
        });

        let extensions = rewrite_sinks(vec![extension])?;

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
            physical_planner::PlanToGraphVisitor::new(&self.schema_provider, &session_state);
        for ext in extensions {
            plan_to_graph_visitor.add_plan(ext)?;
        }
        let graph = plan_to_graph_visitor.into_graph();

        let mut program = LogicalProgram::new(graph, ProgramConfig::default());
        program.optimize(&ChainingOptimizer {});

        Ok(Box::new(InsertStatementPlan::new(
            program,
            used_connections,
        )))
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

    fn visit_insert_statement(
        &self,
        stmt: &InsertStatement,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        match self.build_insert_plan(stmt) {
            Ok(plan) => StatementVisitorResult::Plan(plan),
            Err(e) => panic!("Failed to build INSERT plan: {e}"),
        }
    }
}
