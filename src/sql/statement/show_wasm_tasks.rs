use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

#[derive(Debug, Clone, Default)]
pub struct ShowWasmTasks;

impl ShowWasmTasks {
    pub fn new() -> Self {
        Self
    }
}

impl Statement for ShowWasmTasks {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_show_wasm_tasks(self, context)
    }
}
