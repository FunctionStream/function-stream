use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug, Clone, Default)]
pub struct ShowWasmTasksPlan {
    pub filter: Option<String>,
}

impl ShowWasmTasksPlan {
    pub fn new() -> Self {
        Self { filter: None }
    }
}

impl PlanNode for ShowWasmTasksPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_show_wasm_tasks(self, context)
    }
}
