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

// Function Stream Service handler implementation

use protocol::service::{
    CreateFunctionRequest, Response, SqlRequest, StatusCode,
    function_stream_service_server::FunctionStreamService,
};

use crate::sql::{Coordinator, SqlParser};
use crate::sql::statement::CreateWasmTask;
use std::collections::HashMap;
use uuid::Uuid;

pub struct FunctionStreamServiceImpl {
    coordinator: Coordinator,
}

impl FunctionStreamServiceImpl {
    /// Create a new service instance
    pub fn new() -> Self {
        Self {
            coordinator: Coordinator::new(),
        }
    }
}

#[tonic::async_trait]
impl FunctionStreamService for FunctionStreamServiceImpl {
    async fn execute_sql(
        &self,
        request: tonic::Request<SqlRequest>,
    ) -> Result<tonic::Response<Response>, tonic::Status> {
        // 记录整个SQL请求的开始时间（包括解析和执行）
        let start_time = std::time::Instant::now();
        let req = request.into_inner();
        log::info!("Received SQL request: {}", req.sql);

        // 解析阶段
        let parse_start = std::time::Instant::now();
        let stmt = match SqlParser::parse(&req.sql) {
            Ok(stmt) => {
                let parse_elapsed = parse_start.elapsed().as_secs_f64();
                log::info!("[Timing] SQL parsing: {:.3}s", parse_elapsed);
                stmt
            }
            Err(e) => {
                let parse_elapsed = parse_start.elapsed().as_secs_f64();
                let total_elapsed = start_time.elapsed().as_secs_f64();
                log::warn!(
                    "SQL parse error: {} (parse={:.3}s, total={:.3}s)",
                    e,
                    parse_elapsed,
                    total_elapsed
                );
                return Ok(tonic::Response::new(Response {
                    status_code: StatusCode::BadRequest as i32,
                    message: format!("Parse error: {}", e),
                    data: None,
                }));
            }
        };

        // 协调器执行阶段
        let coord_start = std::time::Instant::now();
        let result = self.coordinator.execute(stmt.as_ref());
        let coord_elapsed = coord_start.elapsed().as_secs_f64();
        log::info!("[Timing] Coordinator execution: {:.3}s", coord_elapsed);

        // 计算总耗时（包括解析和执行）
        let elapsed = start_time.elapsed().as_secs_f64();
        log::info!("SQL request completed: total_elapsed={:.3}s", elapsed);

        let status_code = if result.success {
            StatusCode::Ok
        } else {
            StatusCode::InternalServerError
        };

        Ok(tonic::Response::new(Response {
            status_code: status_code as i32,
            message: result.message,
            data: result.data,
        }))
    }

    async fn create_function(
        &self,
        request: tonic::Request<CreateFunctionRequest>,
    ) -> Result<tonic::Response<Response>, tonic::Status> {
        let start_time = std::time::Instant::now();
        let req = request.into_inner();
        log::info!(
            "Received CreateFunction request: config_bytes size={}, wasm_bytes size={}",
            req.config_bytes.len(),
            req.wasm_bytes.len()
        );

        // Convert bytes to path strings (assuming UTF-8 encoding)
        let config_path = match String::from_utf8(req.config_bytes) {
            Ok(path) => path,
            Err(e) => {
                log::error!("Failed to decode config_bytes as UTF-8: {}", e);
                return Ok(tonic::Response::new(Response {
                    status_code: StatusCode::BadRequest as i32,
                    message: format!("Invalid config_path encoding: {}", e),
                    data: None,
                }));
            }
        };

        let wasm_path = match String::from_utf8(req.wasm_bytes) {
            Ok(path) => path,
            Err(e) => {
                log::error!("Failed to decode wasm_bytes as UTF-8: {}", e);
                return Ok(tonic::Response::new(Response {
                    status_code: StatusCode::BadRequest as i32,
                    message: format!("Invalid wasm_path encoding: {}", e),
                    data: None,
                }));
            }
        };

        // Extract task name from config_path or wasm_path (default to UUID)
        let task_name = std::path::Path::new(&config_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        // Build properties map with wasm-path and config-path
        let mut properties = HashMap::new();
        properties.insert("wasm-path".to_string(), wasm_path);
        properties.insert("config-path".to_string(), config_path);

        // Create CreateWasmTask statement
        let stmt = CreateWasmTask::new(task_name.clone(), properties);

        // Execute using coordinator
        let coord_start = std::time::Instant::now();
        let result = self.coordinator.execute(&stmt as &dyn crate::sql::statement::Statement);
        let coord_elapsed = coord_start.elapsed().as_secs_f64();
        log::info!("[Timing] Coordinator execution: {:.3}s", coord_elapsed);

        // Calculate total elapsed time
        let elapsed = start_time.elapsed().as_secs_f64();
        log::info!("CreateFunction completed: total_elapsed={:.3}s", elapsed);

        let status_code = if result.success {
            StatusCode::Created
        } else {
            StatusCode::InternalServerError
        };

        Ok(tonic::Response::new(Response {
            status_code: status_code as i32,
            message: result.message,
            data: result.data,
        }))
    }
}

impl Default for FunctionStreamServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}
