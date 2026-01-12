mod create_wasm_task;
mod drop_wasm_task;
mod show_wasm_tasks;
mod start_wasm_task;
mod stop_wasm_task;
mod visitor;

pub use create_wasm_task::CreateWasmTask;
pub use drop_wasm_task::DropWasmTask;
pub use show_wasm_tasks::ShowWasmTasks;
pub use start_wasm_task::StartWasmTask;
pub use stop_wasm_task::StopWasmTask;
pub use visitor::{StatementVisitor, StatementVisitorContext, StatementVisitorResult};

use std::fmt;

#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub success: bool,
    pub message: String,
    pub data: Option<String>,
}

impl ExecuteResult {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: None,
        }
    }

    pub fn ok_with_data(message: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: Some(data.into()),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }
}

pub trait Statement: fmt::Debug + Send + Sync {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;
}
