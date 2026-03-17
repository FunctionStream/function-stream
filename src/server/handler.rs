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

use arrow_ipc::writer::StreamWriter;
use log::{error, info};
use tonic::{Request, Response as TonicResponse, Status};

use protocol::service::FunctionInfo as ProtoFunctionInfo;
use protocol::service::{
    CreateFunctionRequest, CreatePythonFunctionRequest, DropFunctionRequest, Response,
    ShowFunctionsRequest, ShowFunctionsResponse, SqlRequest, StartFunctionRequest, StatusCode,
    StopFunctionRequest, function_stream_service_server::FunctionStreamService,
};

use crate::coordinator::Coordinator;
use crate::coordinator::{
    CreateFunction, CreatePythonFunction, DataSet, DropFunction, ShowFunctions,
    ShowFunctionsResult, StartFunction, Statement, StopFunction,
};
use crate::sql::planner::parse::parse_sql;

pub struct FunctionStreamServiceImpl {
    coordinator: Arc<Coordinator>,
}

impl FunctionStreamServiceImpl {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self { coordinator }
    }

    fn build_response(status_code: StatusCode, message: String, data: Option<Vec<u8>>) -> Response {
        Response {
            status_code: status_code as i32,
            message,
            data,
        }
    }

    fn data_set_to_ipc_bytes(ds: &dyn DataSet) -> Option<Vec<u8>> {
        let batch = ds.to_record_batch();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).ok()?;
            writer.write(&batch).ok()?;
            writer.finish().ok()?;
        }
        Some(buf)
    }
}

#[tonic::async_trait]
impl FunctionStreamService for FunctionStreamServiceImpl {
    async fn execute_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();

        let parse_start = Instant::now();
        let statements = match parse_sql(&req.sql) {
            Ok(stmts) => {
                log::debug!(
                    "SQL parsed {} statement(s) in {}ms",
                    stmts.len(),
                    parse_start.elapsed().as_millis()
                );
                stmts
            }
            Err(e) => {
                return Ok(TonicResponse::new(Self::build_response(
                    StatusCode::BadRequest,
                    format!("Parse error: {}", e),
                    None,
                )));
            }
        };

        let exec_start = Instant::now();
        let mut last_result = self.coordinator.execute(statements[0].as_ref());
        for stmt in &statements[1..] {
            if !last_result.success {
                break;
            }
            last_result = self.coordinator.execute(stmt.as_ref());
        }
        let result = last_result;
        log::debug!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            error!("Execution failed: {}", result.message);
            StatusCode::InternalServerError
        };

        log::debug!(
            "Total SQL request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            result
                .data
                .as_ref()
                .and_then(|ds| Self::data_set_to_ipc_bytes(ds.as_ref())),
        )))
    }

    async fn create_function(
        &self,
        request: Request<CreateFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();
        info!(
            "Received CreateFunction request. Config size: {}, Function size: {}",
            req.config_bytes.len(),
            req.function_bytes.len()
        );

        let config_bytes = if !req.config_bytes.is_empty() {
            Some(req.config_bytes)
        } else {
            None
        };

        let stmt = CreateFunction::from_bytes(req.function_bytes, config_bytes);

        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Created
        } else {
            error!("CreateFunction failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!(
            "Total CreateFunction request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            result
                .data
                .as_ref()
                .and_then(|ds| Self::data_set_to_ipc_bytes(ds.as_ref())),
        )))
    }

    async fn create_python_function(
        &self,
        request: Request<CreatePythonFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();
        info!(
            "Received CreatePythonFunction request. Class name: {}, Modules: {}",
            req.class_name,
            req.modules.len()
        );

        // Convert proto modules to PythonModule
        let modules: Vec<crate::coordinator::PythonModule> = req
            .modules
            .into_iter()
            .map(|m| crate::coordinator::PythonModule {
                name: m.module_name,
                bytes: m.module_bytes,
            })
            .collect();

        if modules.is_empty() {
            return Ok(TonicResponse::new(Self::build_response(
                StatusCode::BadRequest,
                "At least one module is required".to_string(),
                None,
            )));
        }

        let stmt = CreatePythonFunction::new(req.class_name, modules, req.config_content);

        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Created
        } else {
            error!("CreatePythonFunction failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!(
            "Total CreatePythonFunction request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            result
                .data
                .as_ref()
                .and_then(|ds| Self::data_set_to_ipc_bytes(ds.as_ref())),
        )))
    }

    async fn drop_function(
        &self,
        request: Request<DropFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();
        info!(
            "Received DropFunction request: function_name={}",
            req.function_name
        );

        let stmt = DropFunction::new(req.function_name);
        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            error!("DropFunction failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!(
            "Total DropFunction request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            None,
        )))
    }

    async fn show_functions(
        &self,
        request: Request<ShowFunctionsRequest>,
    ) -> Result<TonicResponse<ShowFunctionsResponse>, Status> {
        let start_time = Instant::now();
        let _req = request.into_inner();
        info!("Received ShowFunctions request");

        let stmt = ShowFunctions::new();
        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let (status_code, message) = if result.success {
            (StatusCode::Ok as i32, result.message)
        } else {
            error!("ShowFunctions failed: {}", result.message);
            (StatusCode::InternalServerError as i32, result.message)
        };

        let functions: Vec<ProtoFunctionInfo> = result
            .data
            .as_ref()
            .and_then(|arc_ds| {
                (arc_ds.as_ref() as &dyn std::any::Any).downcast_ref::<ShowFunctionsResult>()
            })
            .map(|sfr| {
                sfr.functions()
                    .iter()
                    .map(|f| ProtoFunctionInfo {
                        name: f.name.clone(),
                        task_type: f.task_type.clone(),
                        status: f.status.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        info!(
            "Total ShowFunctions request cost: {}ms, count={}",
            start_time.elapsed().as_millis(),
            functions.len()
        );

        Ok(TonicResponse::new(ShowFunctionsResponse {
            status_code,
            message,
            functions,
        }))
    }

    async fn start_function(
        &self,
        request: Request<StartFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();
        info!(
            "Received StartFunction request: function_name={}",
            req.function_name
        );

        let stmt = StartFunction::new(req.function_name);
        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            error!("StartFunction failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!(
            "Total StartFunction request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            None,
        )))
    }

    async fn stop_function(
        &self,
        request: Request<StopFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();
        info!(
            "Received StopFunction request: function_name={}",
            req.function_name
        );

        let stmt = StopFunction::new(req.function_name);
        let exec_start = Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn Statement);
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            error!("StopFunction failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!(
            "Total StopFunction request cost: {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            None,
        )))
    }
}

impl Default for FunctionStreamServiceImpl {
    fn default() -> Self {
        Self::new(Arc::new(Coordinator::new()))
    }
}
