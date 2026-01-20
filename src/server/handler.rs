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

use log::{error, info, warn};
use tonic::{Request, Response as TonicResponse, Status};

use protocol::service::{
    function_stream_service_server::FunctionStreamService, CreateFunctionRequest, Response,
    SqlRequest, StatusCode,
};

use crate::sql::statement::{CreateFunction, Statement};
use crate::sql::{Coordinator, SqlParser};

pub struct FunctionStreamServiceImpl {
    coordinator: Arc<Coordinator>,
}

impl FunctionStreamServiceImpl {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self { coordinator }
    }

    fn build_response(
        status_code: StatusCode,
        message: String,
        data: Option<String>,
    ) -> Response {
        Response {
            status_code: status_code as i32,
            message,
            data,
        }
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
        
        info!("Received SQL request, length: {}", req.sql.len());

        let parse_start = Instant::now();
        let stmt = match SqlParser::parse(&req.sql) {
            Ok(stmt) => {
                info!("SQL parsed in {}ms", parse_start.elapsed().as_millis());
                stmt
            }
            Err(e) => {
                warn!("SQL parse failed: {}, cost: {}ms", e, parse_start.elapsed().as_millis());
                return Ok(TonicResponse::new(Self::build_response(
                    StatusCode::BadRequest,
                    format!("Parse error: {}", e),
                    None,
                )));
            }
        };

        let exec_start = Instant::now();
        let result = self.coordinator.execute(stmt.as_ref());
        info!(
            "Coordinator execution finished in {}ms",
            exec_start.elapsed().as_millis()
        );

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            error!("Execution failed: {}", result.message);
            StatusCode::InternalServerError
        };

        info!("Total SQL request cost: {}ms", start_time.elapsed().as_millis());

        Ok(TonicResponse::new(Self::build_response(
            status_code,
            result.message,
            result.data,
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
            result.data,
        )))
    }
}

impl Default for FunctionStreamServiceImpl {
    fn default() -> Self {
        Self::new(Arc::new(Coordinator::new()))
    }
}
