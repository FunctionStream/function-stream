mod create_wasm_task_plan;
mod drop_wasm_task_plan;
mod logical_plan_visitor;
mod optimizer;
mod show_wasm_tasks_plan;
mod start_wasm_task_plan;
mod stop_wasm_task_plan;
mod visitor;

pub use create_wasm_task_plan::CreateWasmTaskPlan;
pub use drop_wasm_task_plan::DropWasmTaskPlan;
pub use logical_plan_visitor::LogicalPlanVisitor;
pub use optimizer::LogicalPlanner;
pub use show_wasm_tasks_plan::ShowWasmTasksPlan;
pub use start_wasm_task_plan::StartWasmTaskPlan;
pub use stop_wasm_task_plan::StopWasmTaskPlan;
pub use visitor::{PlanVisitor, PlanVisitorContext, PlanVisitorResult};

use std::fmt;

pub trait PlanNode: fmt::Debug + Send + Sync {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult;
}
