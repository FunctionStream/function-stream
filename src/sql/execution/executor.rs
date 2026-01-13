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
    CreateWasmTaskPlan, DropWasmTaskPlan, PlanNode, PlanVisitor, PlanVisitorContext,
    PlanVisitorResult, ShowWasmTasksPlan, StartWasmTaskPlan, StopWasmTaskPlan,
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
    fn visit_create_wasm_task(
        &self,
        plan: &CreateWasmTaskPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        let start_time = std::time::Instant::now();
        log::info!("Executing CREATE WASMTASK (name will be read from config file)");

        // 读取 WASM 文件
        let wasm_read_start = std::time::Instant::now();
        let wasm_bytes = match fs::read(&plan.wasm_path) {
            Ok(bytes) => bytes,
            Err(e) => {
                return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                    "Failed to read WASM file: {}: {}",
                    plan.wasm_path, e
                ))));
            }
        };
        let wasm_read_elapsed = wasm_read_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] WASM file read: {:.3}s (size: {} bytes)",
            wasm_read_elapsed,
            wasm_bytes.len()
        );

        // 读取配置文件（如果存在）
        let config_read_start = std::time::Instant::now();
        let config_bytes = match if let Some(ref config_path) = plan.config_path {
            match fs::read(config_path) {
                Ok(bytes) => {
                    let elapsed = config_read_start.elapsed().as_secs_f64();
                    log::info!("[Timing] Config file read: {:.3}s", elapsed);
                    Ok(bytes)
                }
                Err(e) => Err(ExecuteError::new(format!(
                    "Failed to read config file: {}: {}",
                    config_path, e
                ))),
            }
        } else {
            // 如果没有配置文件，创建一个包含任务名称和属性的默认配置
            let mut config = std::collections::HashMap::new();
            let default_name = std::path::Path::new(&plan.wasm_path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("default-task")
                .to_string();
            config.insert("name".to_string(), default_name.clone());
            for (k, v) in &plan.properties {
                config.insert(k.clone(), v.clone());
            }
            match serde_yaml::to_string(&config) {
                Ok(s) => {
                    let bytes = s.into_bytes();
                    let elapsed = config_read_start.elapsed().as_secs_f64();
                    log::info!("[Timing] Default config generation: {:.3}s", elapsed);
                    Ok(bytes)
                }
                Err(e) => Err(ExecuteError::new(format!(
                    "Failed to serialize default config: {}",
                    e
                ))),
            }
        } {
            Ok(bytes) => bytes,
            Err(e) => {
                return PlanVisitorResult::Execute(Err(e));
            }
        };

        // 注册任务（name 从 YAML 配置中解析）
        log::debug!(
            "Registering task with config size={} bytes, wasm size={} bytes",
            config_bytes.len(),
            wasm_bytes.len()
        );
        let register_start = std::time::Instant::now();

        if let Err(e) = self.task_manager.register_task(&config_bytes, &wasm_bytes) {
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
        log::info!("[Timing] CREATE WASMTASK total: {:.3}s", total_elapsed);

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(
            "WASMTASK created successfully (name from config file)",
        )))
    }

    fn visit_drop_wasm_task(
        &self,
        plan: &DropWasmTaskPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing DROP WASMTASK: {}", plan.name);

        // 如果 force=true，先停止任务
        if plan.force {
            let _ = self.task_manager.stop_task(&plan.name);
            let _ = self.task_manager.close_task(&plan.name);
        } else {
            // 检查任务状态，如果正在运行则报错
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

        // 删除任务
        if let Err(e) = self.task_manager.remove_task(&plan.name) {
            return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                "Failed to remove task: {}: {}",
                plan.name, e
            ))));
        }

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(format!(
            "WASMTASK '{}' dropped successfully",
            plan.name
        ))))
    }

    fn visit_start_wasm_task(
        &self,
        plan: &StartWasmTaskPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing START WASMTASK: {}", plan.name);

        if let Err(e) = self.task_manager.start_task(&plan.name) {
            return PlanVisitorResult::Execute(Err(ExecuteError::new(format!(
                "Failed to start task: {}: {}",
                plan.name, e
            ))));
        }

        PlanVisitorResult::Execute(Ok(ExecuteResult::ok(format!(
            "WASMTASK '{}' started successfully",
            plan.name
        ))))
    }

    fn visit_stop_wasm_task(
        &self,
        plan: &StopWasmTaskPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!(
            "Executing STOP WASMTASK: {} (graceful: {})",
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
            "WASMTASK '{}' stopped successfully",
            plan.name
        ))))
    }

    fn visit_show_wasm_tasks(
        &self,
        _plan: &ShowWasmTasksPlan,
        _context: &PlanVisitorContext,
    ) -> PlanVisitorResult {
        log::info!("Executing SHOW WASMTASKS");

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
