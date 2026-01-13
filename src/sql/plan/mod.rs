// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
