use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

#[derive(Debug, Clone)]
pub struct StopWasmTask {
    pub name: String,
}

impl StopWasmTask {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Statement for StopWasmTask {
    fn accept(&self, visitor: &dyn StatementVisitor, context: &StatementVisitorContext) -> StatementVisitorResult {
        visitor.visit_stop_wasm_task(self, context)
    }
}


