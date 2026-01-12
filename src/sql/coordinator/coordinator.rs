use std::sync::Arc;

use crate::runtime::taskexecutor::TaskManager;
use crate::sql::analyze::Analyzer;
use crate::sql::execution::Executor;
use crate::sql::plan::{LogicalPlanVisitor, LogicalPlanner};
use crate::sql::statement::{ExecuteResult, Statement};

use super::execution_context::ExecutionContext;

/// Coordinator 负责协调 SQL 语句的执行
pub struct Coordinator;

impl Coordinator {
    pub fn new() -> Self {
        Self
    }

    /// 执行 Statement，保证返回 ExecuteResult，不抛出异常
    pub fn execute(&self, stmt: &dyn Statement) -> ExecuteResult {
        let start_time = std::time::Instant::now();
        let context = ExecutionContext::new();
        let execution_id = context.execution_id;

        log::info!(
            "[ExecutionStart] execution_id={}, statement={:?}",
            execution_id,
            std::any::type_name_of_val(&stmt)
        );

        // 1. 分析阶段
        let analyze_start = std::time::Instant::now();
        let analyzer = Analyzer::new(&context);
        let analysis = match analyzer.analyze(stmt) {
            Ok(a) => a,
            Err(e) => {
                let total_elapsed = start_time.elapsed().as_secs_f64();
                log::error!(
                    "[ExecutionEnd] execution_id={}, error={}, total_elapsed={:.3}s",
                    execution_id,
                    e,
                    total_elapsed
                );
                return ExecuteResult::err(format!("Analyze error: {}", e));
            }
        };
        log::info!(
            "[Timing] execution_id={}, analyze={:.3}s",
            execution_id,
            analyze_start.elapsed().as_secs_f64()
        );

        // 2. 逻辑计划生成阶段
        let plan_start = std::time::Instant::now();
        let logical_plan_visitor = LogicalPlanVisitor::new();
        let plan = logical_plan_visitor.visit(&analysis);
        log::info!(
            "[Timing] execution_id={}, logical_plan={:.3}s",
            execution_id,
            plan_start.elapsed().as_secs_f64()
        );

        // 3. 优化阶段
        let optimize_start = std::time::Instant::now();
        let logical_planner = LogicalPlanner::new();
        let optimized_plan = logical_planner.optimize(plan, &analysis);
        log::info!(
            "[Timing] execution_id={}, optimize={:.3}s",
            execution_id,
            optimize_start.elapsed().as_secs_f64()
        );

        // 4. 获取 TaskManager
        let tm_start = std::time::Instant::now();
        let task_manager = match TaskManager::get() {
            Ok(tm) => tm,
            Err(e) => {
                let total_elapsed = start_time.elapsed().as_secs_f64();
                log::error!(
                    "[ExecutionEnd] execution_id={}, error=Failed to get TaskManager: {}, total_elapsed={:.3}s",
                    execution_id,
                    e,
                    total_elapsed
                );
                return ExecuteResult::err(format!("Failed to get TaskManager: {}", e));
            }
        };
        log::info!(
            "[Timing] execution_id={}, task_manager_get={:.3}s",
            execution_id,
            tm_start.elapsed().as_secs_f64()
        );

        // 5. 执行
        let exec_start = std::time::Instant::now();
        let executor = Executor::new(task_manager);
        let result = match executor.execute(optimized_plan.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                let total_elapsed = start_time.elapsed().as_secs_f64();
                log::error!(
                    "[ExecutionEnd] execution_id={}, error={}, total_elapsed={:.3}s",
                    execution_id,
                    e,
                    total_elapsed
                );
                return ExecuteResult::err(format!("Execute error: {}", e));
            }
        };
        log::info!(
            "[Timing] execution_id={}, execute={:.3}s",
            execution_id,
            exec_start.elapsed().as_secs_f64()
        );

        let total_elapsed = start_time.elapsed().as_secs_f64();
        log::info!(
            "[ExecutionEnd] execution_id={}, success={}, total_elapsed={:.3}s",
            execution_id,
            result.success,
            total_elapsed
        );

        result
    }

    /// 获取 TaskManager
    pub fn task_manager(&self) -> Option<Arc<TaskManager>> {
        TaskManager::get().ok()
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statement::CreateWasmTask;
    use std::collections::HashMap;

    #[test]
    fn test_coordinator_analyze_error() {
        let coordinator = Coordinator::new();

        // Missing wasm-path - should return error in ExecuteResult, not panic
        let props = HashMap::new();
        let stmt = CreateWasmTask::new("my_task".to_string(), props);
        let result = coordinator.execute(&stmt as &dyn Statement);
        assert!(!result.success);
    }
}
