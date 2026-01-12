use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateWasmTaskPlan {
    pub name: String,
    pub wasm_path: String,
    pub config_path: Option<String>,
    pub properties: HashMap<String, String>,
}

impl CreateWasmTaskPlan {
    pub fn new(
        name: String,
        wasm_path: String,
        config_path: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            wasm_path,
            config_path,
            properties,
        }
    }
}

impl PlanNode for CreateWasmTaskPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_create_wasm_task(self, context)
    }
}
