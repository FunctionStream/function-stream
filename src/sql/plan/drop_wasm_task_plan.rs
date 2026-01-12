use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug, Clone)]
pub struct DropWasmTaskPlan {
    pub name: String,
    pub force: bool,
}

impl DropWasmTaskPlan {
    pub fn new(name: String) -> Self {
        Self { name, force: false }
    }

    pub fn with_force(name: String, force: bool) -> Self {
        Self { name, force }
    }
}

impl PlanNode for DropWasmTaskPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_drop_wasm_task(self, context)
    }
}
