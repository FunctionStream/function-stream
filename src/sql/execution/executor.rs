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

use crate::runtime::taskexecutor::TaskManager;
use crate::sql::plan::{
    CreateFunctionPlan, DropFunctionPlan, PlanNode, PlanVisitor, PlanVisitorContext,
    PlanVisitorResult, ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan,
};
use crate::sql::statement::ExecuteResult;
use std::fmt;
use std::fs;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ExecuteError {
    pub message: String,
}

impl ExecuteError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Execute error: {}", self.message)
    }
}

impl std::error::Error for ExecuteError {}

pub struct Executor {
    task_manager: Arc<TaskManager>,
}

impl Executor {
    pub fn new(task_manager: Arc<TaskManager>) -> Self {
        Self { task_manager }
    }

    pub fn execute(&self, plan: &dyn PlanNode) -> Result<ExecuteResult, ExecuteError> {
        let exec_start = std::time::Instant::now();
        let context = PlanVisitorContext::new();

        let visitor_result = plan.accept(self, &context);

        let exec_elapsed = exec_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] Executor.execute (plan routing + execution): {:.3}s",
            exec_elapsed
        );

        match visitor_result {
            PlanVisitorResult::Execute(result) => result,
        }
    }
}

impl PlanVisitor for Executor {
    fn visit_create_function(
        &self,
        plan: &CreateFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let start_time = std::time::Instant::now();
        log::info!("Executing CREATE FUNCTION (name will be read from config file)");

        let function_bytes = match &plan.function_source {
            crate::sql::statement::FunctionSource::Path(path) => {
                match fs::read(path) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                            "Failed to read function file: {}: {}",
                            path, e
                        ))));
                    }
                }
            }
            crate::sql::statement::FunctionSource::Bytes(bytes) => bytes.clone(),
        };

        let config_bytes = match &plan.config_source {
            Some(crate::sql::statement::ConfigSource::Path(path)) => {
                match fs::read(path) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                            "Failed to read config file: {}: {}",
                            path, e
                        ))));
                    }
                }
            }
            Some(crate::sql::statement::ConfigSource::Bytes(bytes)) => bytes.clone(),
            None => {
                return PlanVisitorResult::Execute(Err(ExecuteError::new(
                    "Config is required but not provided".to_string(),
                )));
            }
        };

        log::debug!(
            "Registering task with config size={} bytes, function size={} bytes",
            config_bytes.len(),
            function_bytes.len()
        );
        let register_start = std::time::Instant::now();

        if let Err(e) = self.task_manager.register_task(&config_bytes, &function_bytes) {
            let error_msg = "Failed to register task (name read from config file)".to_string();
            log::error!("{}: {}", error_msg, e);
            log::error!("Error chain: {:?}", e);
            let mut full_error = format!("{}: {}", error_msg, e);
            let mut source = e.source();
            let mut depth = 0;
            while let Some(err) = source {
                depth += 1;
                full_error.push_str(&format!("\n  Caused by ({}): {}", depth, err));
                source = err.source();
                if depth > 10 {
                    full_error.push_str("\n  ... (error chain too long, truncated)");
                    break;
                }
            }
            log::error!("Full error details:\n{}", full_error);
            return PlanVisitorResult::Execute(Err(ExecuteError::new(&full_error)));
        }

        let register_elapsed = register_start.elapsed().as_secs_f64();
        log::info!("[Timing] Task registration: {:.3}s", register_elapsed);

        let total_elapsed = start_time.elapsed().as_secs_f64();
        log::info!("[Timing] CREATE FUNCTION total: {:.3}s", total_elapsed);

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(
            "FUNCTION created successfully (name from config file)",
        )))
    }

    fn visit_drop_function(
        &self,
        plan: &DropFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing DROP FUNCTION: {}", plan.name);

        if plan.force {
            let _ = self.task_manager.stop_task(&plan.name);
            let _ = self.task_manager.close_task(&plan.name);
        } else {
            let status = match self.task_manager.get_task_status(&plan.name) {
                Ok(s) => s,
                Err(e) => {
                    return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                        "Task '{}' not found: {}",
                        plan.name, e
                    ))));
                }
            };
            if status.is_running() {
                return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                    "Task '{}' is running. Use FORCE to drop running task.",
                    plan.name
                ))));
            }
        }

        if let Err(e) = self.task_manager.remove_task(&plan.name) {
            return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                "Failed to remove task: {}: {}",
                plan.name, e
            ))));
        }

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(format!(
            "FUNCTION '{}' dropped successfully",
            plan.name
        ))))
    }

    fn visit_start_function(
        &self,
        plan: &StartFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing START FUNCTION: {}", plan.name);

        if let Err(e) = self.task_manager.start_task(&plan.name) {
            return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                "Failed to start task: {}: {}",
                plan.name, e
            ))));
        }

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(format!(
            "FUNCTION '{}' started successfully",
            plan.name
        ))))
    }

    fn visit_stop_function(
        &self,
        plan: &StopFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!(
            "Executing STOP FUNCTION: {} (graceful: {})",
            plan.name,
            plan.graceful
        );

        if let Err(e) = self.task_manager.stop_task(&plan.name) {
            return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                "Failed to stop task: {}: {}",
                plan.name, e
            ))));
        }

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(format!(
            "FUNCTION '{}' stopped successfully",
            plan.name
        ))))
    }

    fn visit_show_functions(
        &self,
        _plan: &ShowFunctionsPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing SHOW FUNCTIONS");

        let task_names = self.task_manager.list_tasks();

        let json_objects: Vec<String> = task_names
            .iter()
            .map(|name| {
                let status_str = self
                    .task_manager
                    .get_task_status(name)
                    .map(|s| format!("{:?}", s))
                    .unwrap_or_else(|_| "UNKNOWN".to_string());
                format!(r#"{{"name":"{}","status":"{}"}}"#, name, status_str)
            })
            .collect();
        let data = format!("[{}]", json_objects.join(","));

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok_with_data(
            format!("{} task(s) found", task_names.len()),
            data,
        )))
    }
}
