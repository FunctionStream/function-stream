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
use tonic::{Request, Response as TonicResponse, Status};
use tracing::{debug, error, info, warn};

use protocol::service::FunctionInfo as ProtoFunctionInfo;
use protocol::service::{
    CreateFunctionRequest, CreatePythonFunctionRequest, DropFunctionRequest, Response,
    ShowFunctionsRequest, ShowFunctionsResponse, SqlRequest, StartFunctionRequest, StatusCode,
    StopFunctionRequest, function_stream_service_server::FunctionStreamService,
};

use crate::coordinator::{
    Coordinator, CreateFunction, CreatePythonFunction, DataSet, DropFunction, PythonModule,
    ShowFunctions, ShowFunctionsResult, StartFunction, Statement, StopFunction,
};
use crate::sql::parse::parse_sql;

pub struct FunctionStreamServiceImpl {
    coordinator: Arc<Coordinator>,
}

impl FunctionStreamServiceImpl {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self { coordinator }
    }

    fn serialize_dataset(ds: &dyn DataSet) -> Result<Vec<u8>, String> {
        let batch = ds.to_record_batch();
        let mut buf = Vec::new();

        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| format!("IPC writer initialization failed: {e}"))?;

        writer
            .write(&batch)
            .map_err(|e| format!("IPC write failed: {e}"))?;

        writer
            .finish()
            .map_err(|e| format!("IPC finish failed: {e}"))?;

        Ok(buf)
    }

    fn build_success_response(
        status: StatusCode,
        message: String,
        data: Option<Arc<dyn DataSet>>,
    ) -> Response {
        let payload = match data {
            Some(ds) => match Self::serialize_dataset(ds.as_ref()) {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    error!("Data serialization error: {}", e);
                    return Self::build_error_response(
                        StatusCode::InternalServerError,
                        "Internal data serialization error".to_string(),
                    );
                }
            },
            None => None,
        };

        Response {
            status_code: status as i32,
            message,
            data: payload,
        }
    }

    fn build_error_response(status: StatusCode, message: String) -> Response {
        Response {
            status_code: status as i32,
            message,
            data: None,
        }
    }

    async fn execute_statement(
        &self,
        stmt: &dyn Statement,
        success_status: StatusCode,
    ) -> Response {
        let result = self.coordinator.execute_with_stream_catalog(stmt).await;

        if result.success {
            Self::build_success_response(success_status, result.message, result.data)
        } else {
            Self::build_error_response(StatusCode::InternalServerError, result.message)
        }
    }
}

#[tonic::async_trait]
impl FunctionStreamService for FunctionStreamServiceImpl {
    async fn execute_sql(
        &self,
        request: Request<SqlRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        let statements = parse_sql(&req.sql).map_err(|e| {
            let detail = e.to_string();
            warn!("SQL parse rejection: {}", detail);
            Status::invalid_argument(detail)
        })?;

        if statements.is_empty() {
            return Ok(TonicResponse::new(Self::build_success_response(
                StatusCode::Ok,
                "No statements executed".to_string(),
                None,
            )));
        }

        let mut final_response = None;

        for stmt in statements {
            let result = self
                .coordinator
                .execute_with_stream_catalog(stmt.as_ref())
                .await;

            if !result.success {
                error!("SQL execution aborted: {}", result.message);
                return Ok(TonicResponse::new(Self::build_error_response(
                    StatusCode::InternalServerError,
                    result.message,
                )));
            }

            final_response = Some(result);
        }

        let result = final_response.unwrap();
        let response = Self::build_success_response(StatusCode::Ok, result.message, result.data);

        debug!("execute_sql completed in {}ms", timer.elapsed().as_millis());
        Ok(TonicResponse::new(response))
    }

    async fn create_function(
        &self,
        request: Request<CreateFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        let config_bytes = (!req.config_bytes.is_empty()).then_some(req.config_bytes);
        let stmt = CreateFunction::from_bytes(req.function_bytes, config_bytes);

        let response = self.execute_statement(&stmt, StatusCode::Created).await;

        info!(
            "create_function completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(response))
    }

    async fn create_python_function(
        &self,
        request: Request<CreatePythonFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        if req.modules.is_empty() {
            return Ok(TonicResponse::new(Self::build_error_response(
                StatusCode::BadRequest,
                "Python function creation requires at least one module".to_string(),
            )));
        }

        let modules: Vec<PythonModule> = req
            .modules
            .into_iter()
            .map(|m| PythonModule {
                name: m.module_name,
                bytes: m.module_bytes,
            })
            .collect();

        let stmt = CreatePythonFunction::new(req.class_name, modules, req.config_content);
        let response = self.execute_statement(&stmt, StatusCode::Created).await;

        info!(
            "create_python_function completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(response))
    }

    async fn drop_function(
        &self,
        request: Request<DropFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        let stmt = DropFunction::new(req.function_name);
        let response = self.execute_statement(&stmt, StatusCode::Ok).await;

        info!(
            "drop_function completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(response))
    }

    async fn show_functions(
        &self,
        _request: Request<ShowFunctionsRequest>,
    ) -> Result<TonicResponse<ShowFunctionsResponse>, Status> {
        let timer = Instant::now();
        let stmt = ShowFunctions::new();

        let result = self.coordinator.execute_with_stream_catalog(&stmt).await;

        if !result.success {
            error!("show_functions execution failed: {}", result.message);
            return Ok(TonicResponse::new(ShowFunctionsResponse {
                status_code: StatusCode::InternalServerError as i32,
                message: result.message,
                functions: vec![],
            }));
        }

        let functions = result
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
            "show_functions completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(ShowFunctionsResponse {
            status_code: StatusCode::Ok as i32,
            message: result.message,
            functions,
        }))
    }

    async fn start_function(
        &self,
        request: Request<StartFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        let stmt = StartFunction::new(req.function_name);
        let response = self.execute_statement(&stmt, StatusCode::Ok).await;

        info!(
            "start_function completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(response))
    }

    async fn stop_function(
        &self,
        request: Request<StopFunctionRequest>,
    ) -> Result<TonicResponse<Response>, Status> {
        let timer = Instant::now();
        let req = request.into_inner();

        let stmt = StopFunction::new(req.function_name);
        let response = self.execute_statement(&stmt, StatusCode::Ok).await;

        info!(
            "stop_function completed in {}ms",
            timer.elapsed().as_millis()
        );
        Ok(TonicResponse::new(response))
    }
}

impl Default for FunctionStreamServiceImpl {
    fn default() -> Self {
        Self::new(Arc::new(Coordinator::new()))
    }
}
