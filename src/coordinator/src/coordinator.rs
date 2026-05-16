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
use std::time::Instant;

use anyhow::{Context, Result};

use crate::coordinator::analyze::Analyzer;
use crate::coordinator::dataset::ExecuteResult;
use crate::coordinator::execution::Executor;
use crate::coordinator::plan::{LogicalPlanVisitor, LogicalPlanner, PlanNode};
use crate::coordinator::statement::Statement;
use crate::sql::schema::StreamSchemaProvider;

use super::execution_context::ExecutionContext;
use super::runtime_context::CoordinatorRuntimeContext;

#[derive(Default)]
pub struct Coordinator {}

impl Coordinator {
    pub fn new() -> Self {
        Self {}
    }

    // ========================================================================
    // Plan compilation
    // ========================================================================

    pub fn compile_plan(
        &self,
        stmt: &dyn Statement,
        schema_provider: StreamSchemaProvider,
    ) -> Result<Box<dyn PlanNode>> {
        self.compile_plan_internal(&ExecutionContext::new(), stmt, schema_provider)
    }

    /// Internal pipeline: Analyze → build logical plan → optimize.
    fn compile_plan_internal(
        &self,
        context: &ExecutionContext,
        stmt: &dyn Statement,
        schema_provider: StreamSchemaProvider,
    ) -> Result<Box<dyn PlanNode>> {
        let exec_id = context.execution_id;
        let start = Instant::now();

        let analysis = Analyzer::new(context)
            .analyze(stmt)
            .map_err(|e| anyhow::anyhow!(e))
            .context("Analyzer phase failed")?;
        log::debug!(
            "[{}] Analyze phase finished in {}ms",
            exec_id,
            start.elapsed().as_millis()
        );

        let plan = LogicalPlanVisitor::new(schema_provider).visit(&analysis);

        let opt_start = Instant::now();
        let optimized = LogicalPlanner::new().optimize(plan, &analysis);
        log::debug!(
            "[{}] Optimizer phase finished in {}ms",
            exec_id,
            opt_start.elapsed().as_millis()
        );

        Ok(optimized)
    }

    // ========================================================================
    // Execution
    // ========================================================================

    pub fn execute(&self, stmt: &dyn Statement) -> ExecuteResult {
        match CoordinatorRuntimeContext::try_from_globals() {
            Ok(ctx) => self.execute_with_runtime_context(stmt, &ctx),
            Err(e) => ExecuteResult::err(e.to_string()),
        }
    }

    pub async fn execute_with_stream_catalog(&self, stmt: &dyn Statement) -> ExecuteResult {
        self.execute(stmt)
    }

    /// Same as [`Self::execute`], but uses an explicit [`CoordinatorRuntimeContext`] (e.g. tests or custom wiring).
    pub fn execute_with_runtime_context(
        &self,
        stmt: &dyn Statement,
        runtime: &CoordinatorRuntimeContext,
    ) -> ExecuteResult {
        let start = Instant::now();
        let context = ExecutionContext::new();
        let exec_id = context.execution_id;
        let schema_provider = runtime.planning_schema_provider();

        let result = (|| -> Result<ExecuteResult> {
            let plan = self.compile_plan_internal(&context, stmt, schema_provider)?;

            let exec_start = Instant::now();
            let res = Executor::new(
                Arc::clone(&runtime.task_manager),
                runtime.catalog_manager.clone(),
                Arc::clone(&runtime.job_manager),
            )
            .execute(plan.as_ref())
            .map_err(|e| anyhow::anyhow!(e))
            .context("Executor phase failed")?;

            log::debug!(
                "[{}] Executor phase finished in {}ms",
                exec_id,
                exec_start.elapsed().as_millis()
            );
            Ok(res)
        })();

        match result {
            Ok(res) => {
                log::debug!(
                    "[{}] Execution completed in {}ms",
                    exec_id,
                    start.elapsed().as_millis()
                );
                res
            }
            Err(e) => {
                log::error!(
                    "[{}] Execution failed after {}ms. Error: {:#}",
                    exec_id,
                    start.elapsed().as_millis(),
                    e
                );
                ExecuteResult::err(format!("Execution failed: {:#}", e))
            }
        }
    }
}
