use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

#[derive(Debug, Clone)]
pub struct DropWasmTask {
    pub name: String,
}

impl DropWasmTask {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
impl Statement for DropWasmTask {
    fn accept(&self, visitor: &dyn StatementVisitor, context: &StatementVisitorContext) -> StatementVisitorResult {
        visitor.visit_drop_wasm_task(self, context)
    }
}

