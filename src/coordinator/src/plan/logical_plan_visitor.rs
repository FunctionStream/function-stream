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

use datafusion::sql::sqlparser::ast::{ObjectType, Statement as DFStatement};
use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CompileErrorPlan, CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan,
    DropFunctionPlan, DropStreamingTablePlan, DropTablePlan, PlanNode, ShowCatalogTablesPlan,
    ShowCreateStreamingTablePlan, ShowCreateTablePlan, ShowFunctionsPlan, ShowStreamingTablesPlan,
    StartFunctionPlan, StopFunctionPlan,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, CreateTable, DropFunction, DropStreamingTableStatement,
    DropTableStatement, ShowCatalogTables, ShowCreateStreamingTable, ShowCreateTable,
    ShowFunctions, ShowStreamingTables, StartFunction, StatementVisitor, StatementVisitorContext,
    StatementVisitorResult, StopFunction, StreamingTableStatement,
};
use crate::sql::analysis::StreamSchemaProvider;
use crate::sql::schema::try_compile_connector_create_table;

use super::streaming_compiler::StreamingCompiler;

#[derive(Clone)]
pub struct LogicalPlanVisitor {
    schema_provider: StreamSchemaProvider,
}

impl LogicalPlanVisitor {
    pub fn new(schema_provider: StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    pub fn visit(&self, analysis: &Analysis) -> Box<dyn PlanNode> {
        let stmt = analysis.statement();
        let context = StatementVisitorContext::Empty;

        match stmt.accept(self, &context) {
            StatementVisitorResult::Plan(plan) => plan,
            _ => Box::new(CompileErrorPlan::new(
                "LogicalPlanVisitor did not yield a PlanNode variant for the given statement"
                    .to_string(),
            )),
        }
    }
}

impl StatementVisitor for LogicalPlanVisitor {
    fn visit_create_function(
        &self,
        stmt: &CreateFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(CreateFunctionPlan::new(
            stmt.get_function_source().clone(),
            stmt.get_config_source().cloned(),
            stmt.get_extra_properties().clone(),
        )))
    }

    fn visit_drop_function(
        &self,
        stmt: &DropFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_start_function(
        &self,
        stmt: &StartFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StartFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_stop_function(
        &self,
        stmt: &StopFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(StopFunctionPlan::new(stmt.name.clone())))
    }

    fn visit_show_functions(
        &self,
        _stmt: &ShowFunctions,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowFunctionsPlan::new()))
    }

    fn visit_show_catalog_tables(
        &self,
        _stmt: &ShowCatalogTables,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCatalogTablesPlan::new()))
    }

    fn visit_show_create_table(
        &self,
        stmt: &ShowCreateTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCreateTablePlan::new(stmt.table_name.clone())))
    }

    fn visit_create_python_function(
        &self,
        stmt: &CreatePythonFunction,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(CreatePythonFunctionPlan::new(
            stmt.get_class_name().to_string(),
            stmt.get_modules().to_vec(),
            stmt.get_config_content().to_string(),
        )))
    }

    fn visit_create_table(
        &self,
        stmt: &CreateTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        if let Some(result) =
            try_compile_connector_create_table(&self.schema_provider, &stmt.statement)
        {
            return match result {
                Ok((external_table, if_not_exists)) => StatementVisitorResult::Plan(Box::new(
                    CreateTablePlan::external_table(external_table, if_not_exists),
                )),
                Err(err) => StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(format!(
                    "Ingest table resolution failed - {err:#}"
                )))),
            };
        }

        let schema_compiler = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);
        match schema_compiler.sql_statement_to_plan(stmt.statement.clone()) {
            Ok(logical_plan) => {
                debug!(
                    "Successfully compiled logical DDL topology:\n{}",
                    logical_plan.display_graphviz()
                );
                StatementVisitorResult::Plan(Box::new(CreateTablePlan::new(logical_plan)))
            }
            Err(err) => StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(format!(
                "Logical plan translation failed - {err}"
            )))),
        }
    }

    fn visit_streaming_table_statement(
        &self,
        stmt: &StreamingTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let compiler = StreamingCompiler::new(&self.schema_provider);
        match compiler.compile(stmt) {
            Ok(execution_plan) => StatementVisitorResult::Plan(Box::new(execution_plan)),
            Err(err) => StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(format!(
                "Streaming sink compilation aborted - {err}"
            )))),
        }
    }

    fn visit_drop_table_statement(
        &self,
        stmt: &DropTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let DFStatement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } = &stmt.statement
        else {
            return StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(
                "AST mismatch: expected DROP statement for DropTableStatement".to_string(),
            )));
        };

        if *object_type != ObjectType::Table {
            return StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(format!(
                "Drop target must be of type TABLE, got {object_type:?}"
            ))));
        }
        if names.len() != 1 {
            return StatementVisitorResult::Plan(Box::new(CompileErrorPlan::new(
                "Bulk drop operations are not supported. Specify exactly one table.".to_string(),
            )));
        }

        StatementVisitorResult::Plan(Box::new(DropTablePlan::new(
            names[0].to_string(),
            *if_exists,
        )))
    }

    fn visit_show_streaming_tables(
        &self,
        _stmt: &ShowStreamingTables,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowStreamingTablesPlan::new()))
    }

    fn visit_show_create_streaming_table(
        &self,
        stmt: &ShowCreateStreamingTable,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(ShowCreateStreamingTablePlan::new(
            stmt.table_name.clone(),
        )))
    }

    fn visit_drop_streaming_table(
        &self,
        stmt: &DropStreamingTableStatement,
        _ctx: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        StatementVisitorResult::Plan(Box::new(DropStreamingTablePlan::new(
            stmt.table_name.clone(),
            stmt.if_exists,
        )))
    }
}
