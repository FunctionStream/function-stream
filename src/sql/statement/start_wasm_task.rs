use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

#[derive(Debug, Clone)]
pub struct StartWasmTask {
    pub name: String,
}

impl StartWasmTask {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Statement for StartWasmTask {
    fn accept(&self, visitor: &dyn StatementVisitor, context: &StatementVisitorContext) -> StatementVisitorResult {
        visitor.visit_start_wasm_task(self, context)
    }
}


