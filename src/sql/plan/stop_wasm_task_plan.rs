use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug, Clone)]
pub struct StopWasmTaskPlan {
    pub name: String,
    pub graceful: bool,
}

impl StopWasmTaskPlan {
    pub fn new(name: String) -> Self {
        Self { name, graceful: true }
    }
    
    pub fn with_graceful(name: String, graceful: bool) -> Self {
        Self { name, graceful }
    }
}

impl PlanNode for StopWasmTaskPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_stop_wasm_task(self, context)
    }
}


