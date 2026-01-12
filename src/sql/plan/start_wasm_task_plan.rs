use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug, Clone)]
pub struct StartWasmTaskPlan {
    pub name: String,
}

impl StartWasmTaskPlan {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl PlanNode for StartWasmTaskPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_start_wasm_task(self, context)
    }
}
