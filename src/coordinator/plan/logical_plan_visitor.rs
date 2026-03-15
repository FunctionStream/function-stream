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

use tracing::debug;

use crate::coordinator::analyze::analysis::Analysis;
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, DropFunctionPlan, PlanNode, ShowFunctionsPlan,
    StartFunctionPlan, StopFunctionPlan, StreamingSqlPlan,
};
use crate::coordinator::statement::{
    CreateFunction, CreatePythonFunction, DropFunction, ShowFunctions, StartFunction,
    StatementVisitor, StatementVisitorContext, StatementVisitorResult, StopFunction, StreamingSql,
};
use crate::sql::planner::StreamSchemaProvider;

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

    fn visit_streaming_sql(
        &self,
        stmt: &StreamingSql,
        _context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        let sql_to_rel = datafusion::sql::planner::SqlToRel::new(&self.schema_provider);

        match sql_to_rel.sql_statement_to_plan(stmt.statement.clone()) {
            Ok(plan) => {
                debug!("Logical plan:\n{}", plan.display_graphviz());
                StatementVisitorResult::Plan(Box::new(StreamingSqlPlan::new(plan)))
            }
            Err(e) => {
                panic!("Failed to convert SQL statement to logical plan: {e}");
            }
        }
    }
}
