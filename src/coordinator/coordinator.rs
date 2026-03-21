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

use std::time::Instant;

use anyhow::{Context, Result};

use crate::coordinator::analyze::{Analysis, Analyzer};
use crate::coordinator::dataset::ExecuteResult;
use crate::coordinator::execution::Executor;
use crate::coordinator::plan::{LogicalPlanVisitor, LogicalPlanner, PlanNode};
use crate::coordinator::statement::Statement;
use crate::runtime::taskexecutor::TaskManager;
use crate::sql::schema::StreamSchemaProvider;

use super::execution_context::ExecutionContext;

pub struct Coordinator {}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl Coordinator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, stmt: &dyn Statement) -> ExecuteResult {
        let start_time = Instant::now();
        let context = ExecutionContext::new();
        let execution_id = context.execution_id;

        match self.execute_pipeline(&context, stmt) {
            Ok(result) => {
                log::debug!(
                    "[{}] Execution completed in {}ms",
                    execution_id,
                    start_time.elapsed().as_millis()
                );
                result
            }
            Err(e) => {
                log::error!(
                    "[{}] Execution failed after {}ms. Error: {:#}",
                    execution_id,
                    start_time.elapsed().as_millis(),
                    e
                );
                ExecuteResult::err(format!("Execution failed: {:#}", e))
            }
        }
    }

    fn execute_pipeline(
        &self,
        context: &ExecutionContext,
        stmt: &dyn Statement,
    ) -> Result<ExecuteResult> {
        let analysis = self.step_analyze(context, stmt)?;
        let plan = self.step_build_logical_plan(&analysis)?;
        let optimized_plan = self.step_optimize(&analysis, plan)?;
        self.step_execute(optimized_plan)
    }

    fn step_analyze(&self, context: &ExecutionContext, stmt: &dyn Statement) -> Result<Analysis> {
        let start = Instant::now();
        let analyzer = Analyzer::new(context);
        let result = analyzer
            .analyze(stmt)
            .map_err(|e| anyhow::anyhow!(e))
            .context("Analyzer phase failed");

        log::debug!(
            "[{}] Analyze phase finished in {}ms",
            context.execution_id,
            start.elapsed().as_millis()
        );
        result
    }

    fn step_build_logical_plan(&self, analysis: &Analysis) -> Result<Box<dyn PlanNode>> {
        let schema_provider = StreamSchemaProvider::new();
        let visitor = LogicalPlanVisitor::new(schema_provider);
        let plan = visitor.visit(analysis);
        Ok(plan)
    }

    fn step_optimize(
        &self,
        analysis: &Analysis,
        plan: Box<dyn PlanNode>,
    ) -> Result<Box<dyn PlanNode>> {
        let start = Instant::now();
        let planner = LogicalPlanner::new();
        let optimized = planner.optimize(plan, analysis);

        log::debug!(
            "Optimizer phase finished in {}ms",
            start.elapsed().as_millis()
        );
        Ok(optimized)
    }

    fn step_execute(&self, plan: Box<dyn PlanNode>) -> Result<ExecuteResult> {
        let start = Instant::now();
        let task_manager = match TaskManager::get() {
            Ok(tm) => tm,
            Err(e) => {
                return Ok(ExecuteResult::err(format!(
                    "Failed to get TaskManager: {}",
                    e
                )));
            }
        };
        let executor = Executor::new(task_manager.clone());
        let result = executor
            .execute(plan.as_ref())
            .map_err(|e| anyhow::anyhow!(e))
            .context("Executor phase failed");

        log::debug!(
            "Executor phase finished in {}ms",
            start.elapsed().as_millis()
        );
        result
    }
}
