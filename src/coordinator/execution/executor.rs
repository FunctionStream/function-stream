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

use protocol::grpc::api::FsProgram;
use thiserror::Error;
use tracing::{debug, info};

use crate::coordinator::dataset::{empty_record_batch, ExecuteResult, ShowFunctionsResult};
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, CreateTablePlanBody,
    DropFunctionPlan, DropTablePlan, LookupTablePlan, PlanNode, PlanVisitor, PlanVisitorContext,
    PlanVisitorResult, ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan, StreamingTable,
    StreamingTableConnectorPlan,
};
use crate::coordinator::statement::{ConfigSource, FunctionSource};
use crate::runtime::streaming::job::JobManager;
use crate::runtime::taskexecutor::TaskManager;
use crate::sql::schema::StreamTable;
use crate::storage::stream_catalog::CatalogManager;

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
    catalog_manager: Arc<CatalogManager>,
    job_manager: Arc<JobManager>,
}

impl Executor {
    pub fn new(
        task_manager: Arc<TaskManager>,
        catalog_manager: Arc<CatalogManager>,
        job_manager: Arc<JobManager>,
    ) -> Self {
        Self {
            task_manager,
            catalog_manager,
            job_manager,
        }
    }

    pub fn execute(&self, plan: &dyn PlanNode) -> Result<ExecuteResult, ExecuteError> {
        let timer = std::time::Instant::now();
        let context = PlanVisitorContext::new();

        let visitor_result = plan.accept(self, &context);

        match visitor_result {
            PlanVisitorResult::Execute(result) => {
                debug!(
                    target: "executor",
                    elapsed_ms = timer.elapsed().as_millis(),
                    "Execution completed"
                );
                result
            }
        }
    }
}

impl PlanVisitor for Executor {
    fn visit_create_function(
        &self,
        plan: &CreateFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let function_bytes = match &plan.function_source {
                FunctionSource::Path(path) => std::fs::read(path).map_err(|e| {
                    ExecuteError::Validation(format!("Failed to read function at {path}: {e}"))
                })?,
                FunctionSource::Bytes(bytes) => bytes.clone(),
            };

            let config_bytes = match &plan.config_source {
                Some(ConfigSource::Path(path)) => std::fs::read(path).map_err(|e| {
                    ExecuteError::Validation(format!("Failed to read config at {path}: {e}"))
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
                .map_err(|e| ExecuteError::Task(format!("Registration failed: {e:?}")))?;

            Ok(ExecuteResult::ok_with_data(
                "Function registered successfully",
                empty_record_batch(),
            ))
        };

        PlanVisitorResult::Execute(execute())
    }

    fn visit_drop_function(
        &self,
        plan: &DropFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            self.task_manager
                .remove_task(&plan.name)
                .map_err(|e| ExecuteError::Task(format!("Removal failed: {e}")))?;

            Ok(ExecuteResult::ok_with_data(
                format!("Function '{}' dropped", plan.name),
                empty_record_batch(),
            ))
        };

        PlanVisitorResult::Execute(execute())
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

    fn visit_show_functions(
        &self,
        _plan: &ShowFunctionsPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let functions = self.task_manager.list_all_functions();
        let result = ExecuteResult::ok_with_data(
            format!("Found {} task(s)", functions.len()),
            ShowFunctionsResult::new(functions),
        );

        PlanVisitorResult::Execute(Ok(result))
    }

    fn visit_create_python_function(
        &self,
        plan: &CreatePythonFunctionPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let modules = plan
                .modules
                .iter()
                .map(|m| (m.name.clone(), m.bytes.clone()))
                .collect::<Vec<_>>();

            self.task_manager
                .register_python_task(plan.config_content.as_bytes(), &modules)
                .map_err(|e| ExecuteError::Task(format!("Python registration failed: {e}")))?;

            Ok(ExecuteResult::ok_with_data(
                format!("Python function '{}' deployed", plan.class_name),
                empty_record_batch(),
            ))
        };

        PlanVisitorResult::Execute(execute())
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
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let (table_name, if_not_exists, stream_table) = match &plan.body {
                CreateTablePlanBody::ConnectorSource {
                    source_table,
                    if_not_exists,
                } => {
                    let table_name = source_table.name().to_string();
                    let schema = Arc::new(source_table.produce_physical_schema());
                    let table_instance = StreamTable::Source {
                        name: table_name.clone(),
                        schema,
                        event_time_field: source_table.event_time_field().map(str::to_string),
                        watermark_field: source_table.watermark_field().map(str::to_string),
                    };
                    (table_name, *if_not_exists, table_instance)
                }
                CreateTablePlanBody::DataFusion(_) => {
                    return Err(ExecuteError::Internal(
                        "Operation not supported: Currently, the system strictly supports creating tables backed by an external Connector Source (e.g., Kafka, Postgres). In-memory tables, Views, or CTAS (Create Table As Select) are not supported."
                            .into(),
                    ));
                }
            };

            if if_not_exists && self.catalog_manager.has_stream_table(&table_name) {
                return Ok(ExecuteResult::ok(format!(
                    "Table '{table_name}' already exists (skipped)"
                )));
            }

            self.catalog_manager
                .add_table(stream_table)
                .map_err(|e| {
                    ExecuteError::Internal(format!(
                        "Failed to register connector source table '{table_name}': {e}"
                    ))
                })?;

            Ok(ExecuteResult::ok(format!(
                "Created connector source table '{table_name}'"
            )))
        };

        PlanVisitorResult::Execute(execute())
    }

    fn visit_streaming_table(
        &self,
        plan: &StreamingTable,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let sink = StreamTable::Sink {
                name: plan.name.clone(),
                program: plan.program.clone(),
            };

            self.catalog_manager
                .add_table(sink)
                .map_err(|e| ExecuteError::Internal(e.to_string()))?;

            let fs_program: FsProgram = plan.program.clone().into();
            let job_manager: Arc<JobManager> = Arc::clone(&self.job_manager);

            let job_id = plan.name.clone();
            let job_id = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(job_manager.submit_job(job_id, fs_program))
            })
            .map_err(|e| ExecuteError::Internal(format!("Failed to submit streaming job: {e}")))?;

            info!(
                job_id = %job_id,
                table = %plan.name,
                "Streaming table registered and job submitted"
            );

            Ok(ExecuteResult::ok_with_data(
                format!("Streaming table '{}' created, job_id = {}", plan.name, job_id),
                empty_record_batch(),
            ))
        };

        PlanVisitorResult::Execute(execute())
    }

    fn visit_lookup_table(
        &self,
        _plan: &LookupTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        PlanVisitorResult::Execute(Err(ExecuteError::Internal(
            "LookupTable execution not yet implemented".to_string(),
        )))
    }

    fn visit_streaming_connector_table(
        &self,
        _plan: &StreamingTableConnectorPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        PlanVisitorResult::Execute(Err(ExecuteError::Internal(
            "StreamingTableConnector execution not yet implemented".to_string(),
        )))
    }

    fn visit_drop_table_plan(
        &self,
        plan: &DropTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            self.catalog_manager
                .drop_table(&plan.table_name, plan.if_exists)
                .map_err(|e| ExecuteError::Internal(e.to_string()))?;

            Ok(ExecuteResult::ok(format!(
                "Dropped table '{}'",
                plan.table_name
            )))
        };

        PlanVisitorResult::Execute(execute())
    }
}
