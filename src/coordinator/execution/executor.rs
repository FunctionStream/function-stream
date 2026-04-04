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
use tracing::{debug, info, warn};

use crate::coordinator::dataset::{
    ExecuteResult, ShowCatalogTablesResult, ShowCreateStreamingTableResult, ShowCreateTableResult,
    ShowFunctionsResult, ShowStreamingTablesResult, empty_record_batch,
};
use crate::coordinator::plan::{
    CreateFunctionPlan, CreatePythonFunctionPlan, CreateTablePlan, CreateTablePlanBody,
    DropFunctionPlan, DropStreamingTablePlan, DropTablePlan, LookupTablePlan, PlanNode,
    PlanVisitor, PlanVisitorContext, PlanVisitorResult, ShowCatalogTablesPlan,
    ShowCreateStreamingTablePlan, ShowCreateTablePlan, ShowFunctionsPlan, ShowStreamingTablesPlan,
    StartFunctionPlan, StopFunctionPlan, StreamingTable, StreamingTableConnectorPlan,
};
use crate::coordinator::statement::{ConfigSource, FunctionSource};
use crate::runtime::streaming::job::JobManager;
use crate::runtime::streaming::protocol::control::StopMode;
use crate::runtime::taskexecutor::TaskManager;
use crate::sql::schema::show_create_catalog_table;
use crate::sql::schema::table::Table as CatalogTable;
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

    fn visit_show_catalog_tables(
        &self,
        _plan: &ShowCatalogTablesPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let tables = match self.catalog_manager.list_catalog_tables() {
            Ok(tables) => tables,
            Err(e) => {
                return PlanVisitorResult::Execute(Err(ExecuteError::Internal(e.to_string())));
            }
        };
        let n = tables.len();
        let result = ExecuteResult::ok_with_data(
            format!("{n} stream catalog table(s)"),
            ShowCatalogTablesResult::from_tables(&tables),
        );
        PlanVisitorResult::Execute(Ok(result))
    }

    fn visit_show_create_table(
        &self,
        plan: &ShowCreateTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let t = self
                .catalog_manager
                .get_catalog_table(&plan.table_name)
                .map_err(|e| ExecuteError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    ExecuteError::Validation(format!(
                        "Table '{}' not found in stream catalog",
                        plan.table_name
                    ))
                })?;
            let ddl = show_create_catalog_table(t.as_ref());
            Ok(ExecuteResult::ok_with_data(
                format!("SHOW CREATE TABLE {}", plan.table_name),
                ShowCreateTableResult::new(plan.table_name.clone(), ddl),
            ))
        };
        PlanVisitorResult::Execute(execute())
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
            let (table_name, if_not_exists, catalog_table) = match &plan.body {
                CreateTablePlanBody::ConnectorSource {
                    source_table,
                    if_not_exists,
                } => {
                    let table_name = source_table.name().to_string();
                    let table_instance =
                        CatalogTable::ConnectorTable(source_table.as_ref().clone());
                    (table_name, *if_not_exists, table_instance)
                }
                CreateTablePlanBody::DataFusion(_) => {
                    return Err(ExecuteError::Internal(
                        "Operation not supported: Currently, the system strictly supports creating tables backed by an external Connector Source (e.g., Kafka, Postgres). In-memory tables, Views, or CTAS (Create Table As Select) are not supported."
                            .into(),
                    ));
                }
            };

            if if_not_exists && self.catalog_manager.has_catalog_table(&table_name) {
                return Ok(ExecuteResult::ok(format!(
                    "Table '{table_name}' already exists (skipped)"
                )));
            }

            self.catalog_manager
                .add_catalog_table(catalog_table)
                .map_err(|e| {
                    ExecuteError::Internal(format!(
                        "Failed to register connector source table '{}': {}",
                        table_name, e
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
            let fs_program: FsProgram = plan.program.clone().into();
            let job_manager: Arc<JobManager> = Arc::clone(&self.job_manager);

            let job_id = plan.name.clone();
            let job_id = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(job_manager.submit_job(job_id, fs_program.clone()))
            })
            .map_err(|e| ExecuteError::Internal(format!("Failed to submit streaming job: {e}")))?;

            self.catalog_manager
                .persist_streaming_job(
                    &plan.name,
                    &fs_program,
                    plan.comment.as_deref().unwrap_or(""),
                )
                .map_err(|e| {
                    ExecuteError::Internal(format!(
                        "Streaming job '{}' submitted but persistence failed: {e}",
                        plan.name
                    ))
                })?;

            info!(
                job_id = %job_id,
                table = %plan.name,
                "Streaming job submitted and persisted"
            );

            Ok(ExecuteResult::ok_with_data(
                format!(
                    "Streaming table '{}' created, job_id = {}",
                    plan.name, job_id
                ),
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
                .drop_catalog_table(&plan.table_name, plan.if_exists)
                .map_err(|e| ExecuteError::Internal(e.to_string()))?;

            Ok(ExecuteResult::ok(format!(
                "Dropped table '{}'",
                plan.table_name
            )))
        };

        PlanVisitorResult::Execute(execute())
    }

    fn visit_show_streaming_tables(
        &self,
        _plan: &ShowStreamingTablesPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let jobs = self.job_manager.list_jobs();
            let n = jobs.len();
            Ok(ExecuteResult::ok_with_data(
                format!("{n} streaming table(s)"),
                ShowStreamingTablesResult::new(jobs),
            ))
        };
        PlanVisitorResult::Execute(execute())
    }

    fn visit_show_create_streaming_table(
        &self,
        plan: &ShowCreateStreamingTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let detail = self
                .job_manager
                .get_job_detail(&plan.table_name)
                .ok_or_else(|| {
                    ExecuteError::Validation(format!(
                        "Streaming table '{}' not found in active jobs",
                        plan.table_name
                    ))
                })?;

            let pipeline_lines: Vec<String> = detail
                .pipelines
                .iter()
                .map(|p| format!("  pipeline[{}]: {}", p.pipeline_id, p.status))
                .collect();
            let pipeline_detail = if pipeline_lines.is_empty() {
                "(no pipelines)".to_string()
            } else {
                pipeline_lines.join("\n")
            };

            Ok(ExecuteResult::ok_with_data(
                format!("SHOW CREATE STREAMING TABLE {}", plan.table_name),
                ShowCreateStreamingTableResult::new(
                    plan.table_name.clone(),
                    detail.status.to_string(),
                    pipeline_detail,
                    detail.program,
                ),
            ))
        };
        PlanVisitorResult::Execute(execute())
    }

    fn visit_drop_streaming_table(
        &self,
        plan: &DropStreamingTablePlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let execute = || -> Result<ExecuteResult, ExecuteError> {
            let job_exists = self.job_manager.has_job(&plan.table_name);

            if !job_exists && !plan.if_exists {
                return Err(ExecuteError::Validation(format!(
                    "Streaming table '{}' not found in active jobs",
                    plan.table_name
                )));
            }

            if job_exists {
                let job_manager = Arc::clone(&self.job_manager);
                let table_name = plan.table_name.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(job_manager.remove_job(&table_name, StopMode::Graceful))
                })
                .map_err(|e| {
                    ExecuteError::Internal(format!(
                        "Failed to stop streaming job '{}': {}",
                        plan.table_name, e
                    ))
                })?;

                info!(
                    table = %plan.table_name,
                    "Streaming job stopped and removed"
                );
            }

            if let Err(e) = self.catalog_manager.remove_streaming_job(&plan.table_name) {
                warn!(
                    table = %plan.table_name,
                    error = %e,
                    "Failed to remove streaming job persisted definition (non-fatal)"
                );
            }

            let _ = self
                .catalog_manager
                .drop_catalog_table(&plan.table_name, true);

            if job_exists {
                Ok(ExecuteResult::ok(format!(
                    "Dropped streaming table '{}'",
                    plan.table_name
                )))
            } else {
                Ok(ExecuteResult::ok(format!(
                    "Streaming table '{}' does not exist (skipped)",
                    plan.table_name
                )))
            }
        };

        PlanVisitorResult::Execute(execute())
    }
}
