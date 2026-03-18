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

use crate::coordinator::dataset::{ExecuteResult, ShowFunctionsResult, empty_record_batch};
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, DropFunctionPlan,
    LookupTablePlan, PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult,
    ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan, StreamingTable,
    StreamingTableConnectorPlan,
};
use crate::coordinator::statement::{ConfigSource, FunctionSource};
use crate::runtime::taskexecutor::TaskManager;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};
use crate::datastream::logical::{LogicalProgram, ProgramConfig};
use crate::datastream::optimizers::ChainingOptimizer;
use crate::sql::CompiledSql;
use crate::sql::planner::{physical_planner, rewrite_sinks};

#[derive(Error, Debug)]
pub enum ExecuteError {
    #[error("Execution failed: {0}")]
    Internal(String),
    #[error("IO error during execution: {0}")]
    Io(#[from] std::io::Error),
    #[error("Task manager error: {0}")]
    Task(String),
    #[error("Validation error: {0}")]
    Validation(String),
}

pub struct Executor {
    task_manager: Arc<TaskManager>,
}

impl Executor {
    pub fn new(task_manager: Arc<TaskManager>) -> Self {
        Self { task_manager }
    }

    pub fn execute(&self, plan: &dyn PlanNode) -> Result<ExecuteResult, ExecuteError> {
        let timer = std::time::Instant::now();
        let context = PlanVisitorContext::new();

        let visitor_result = plan.accept(self, &context);

        match visitor_result {
            PlanVisitorResult::Execute(result) => {
                let elapsed = timer.elapsed();
                debug!(target: "executor", elapsed_ms = elapsed.as_millis(), "Execution completed");
                result
            }
        }
    }
}

impl PlanVisitor for Executor {
    #[allow(clippy::redundant_closure_call)]
    fn visit_create_function(
        &self,
        plan: &CreateFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = (|| -> Result<ExecuteResult, ExecuteError> {
            let function_bytes = match &plan.function_source {
                FunctionSource::Path(path) => std::fs::read(path).map_err(|e| {
                    ExecuteError::Validation(format!("Failed to read function at {}: {}", path, e))
                })?,
                FunctionSource::Bytes(bytes) => bytes.clone(),
            };

            let config_bytes = match &plan.config_source {
                Some(ConfigSource::Path(path)) => std::fs::read(path).map_err(|e| {
                    ExecuteError::Validation(format!("Failed to read config at {}: {}", path, e))
                })?,
                Some(ConfigSource::Bytes(bytes)) => bytes.clone(),
                None => {
                    return Err(ExecuteError::Validation(
                        "Configuration bytes required for function creation".into(),
                    ));
                }
            };

            info!(config_size = config_bytes.len(), "Registering Wasm task");
            self.task_manager
                .register_task(&config_bytes, &function_bytes)
                .map_err(|e| ExecuteError::Task(format!("Registration failed: {:?}", e)))?;

            Ok(ExecuteResult::ok_with_data(
                "Function registered successfully",
                empty_record_batch(),
            ))
        })();

        PlanVisitorResult::Execute(result)
    }

    #[allow(clippy::redundant_closure_call)]
    fn visit_drop_function(
        &self,
        plan: &DropFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = (|| -> Result<ExecuteResult, ExecuteError> {
            self.task_manager
                .remove_task(&plan.name)
                .map_err(|e| ExecuteError::Task(format!("Removal failed: {}", e)))?;

            Ok(ExecuteResult::ok_with_data(
                format!("Function '{}' dropped", plan.name),
                empty_record_batch(),
            ))
        })();

        PlanVisitorResult::Execute(result)
    }

    fn visit_start_function(
        &self,
        plan: &StartFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = self
            .task_manager
            .start_task(&plan.name)
            .map(|_| {
                ExecuteResult::ok_with_data(
                    format!("Function '{}' started", plan.name),
                    empty_record_batch(),
                )
            })
            .map_err(|e| ExecuteError::Task(e.to_string()));

        PlanVisitorResult::Execute(result)
    }

    #[allow(clippy::redundant_closure_call)]
    fn visit_show_functions(
        &self,
        _plan: &ShowFunctionsPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = {
            let functions = self.task_manager.list_all_functions();

            Ok(ExecuteResult::ok_with_data(
                format!("Found {} task(s)", functions.len()),
                ShowFunctionsResult::new(functions),
            ))
        };

        PlanVisitorResult::Execute(result)
    }

    #[allow(clippy::redundant_closure_call)]
    fn visit_create_python_function(
        &self,
        plan: &CreatePythonFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = (|| -> Result<ExecuteResult, ExecuteError> {
            let modules: Vec<(String, Vec<u8>)> = plan
                .modules
                .iter()
                .map(|m| (m.name.clone(), m.bytes.clone()))
                .collect();

            self.task_manager
                .register_python_task(plan.config_content.as_bytes(), &modules)
                .map_err(|e| ExecuteError::Task(format!("Python registration failed: {}", e)))?;

            Ok(ExecuteResult::ok_with_data(
                format!("Python function '{}' deployed", plan.class_name),
                empty_record_batch(),
            ))
        })();

        PlanVisitorResult::Execute(result)
    }

    fn visit_stop_function(
        &self,
        plan: &StopFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = self
            .task_manager
            .stop_task(&plan.name)
            .map(|_| {
                ExecuteResult::ok_with_data(
                    format!("Function '{}' stopped", plan.name),
                    empty_record_batch(),
                )
            })
            .map_err(|e| ExecuteError::Task(e.to_string()));

        PlanVisitorResult::Execute(result)
    }

    fn visit_create_table_plan(
        &self,
        plan: &CreateTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        // TODO: register table in catalog and execute DDL
        let result = Err(ExecuteError::Internal(format!(
            "CREATE TABLE execution not yet implemented. LogicalPlan:\n{}",
            plan.logical_plan.display_indent()
        )));
        PlanVisitorResult::Execute(result)
    }

    fn visit_streaming_table(
        &self,
        _plan: &StreamingTable,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = Err(ExecuteError::Internal(
            "StreamingTable execution not yet implemented".to_string(),
        ));
        PlanVisitorResult::Execute(result)
    }

    fn visit_lookup_table(
        &self,
        _plan: &LookupTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = Err(ExecuteError::Internal(
            "LookupTable execution not yet implemented".to_string(),
        ));
        PlanVisitorResult::Execute(result)
    }

    fn visit_streaming_connector_table(
        &self,
        _plan: &StreamingTableConnectorPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let result = Err(ExecuteError::Internal(
            "StreamingTableConnector execution not yet implemented".to_string(),
        ));
        PlanVisitorResult::Execute(result)
    }
}
