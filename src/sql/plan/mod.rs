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

mod create_function_plan;
mod drop_function_plan;
mod logical_plan_visitor;
mod optimizer;
mod show_functions_plan;
mod start_function_plan;
mod stop_function_plan;
mod visitor;

pub use create_function_plan::CreateFunctionPlan;
pub use drop_function_plan::DropFunctionPlan;
pub use logical_plan_visitor::LogicalPlanVisitor;
pub use optimizer::LogicalPlanner;
pub use show_functions_plan::ShowFunctionsPlan;
pub use start_function_plan::StartFunctionPlan;
pub use stop_function_plan::StopFunctionPlan;
pub use visitor::{PlanVisitor, PlanVisitorContext, PlanVisitorResult};

use std::fmt;

pub trait PlanNode: fmt::Debug + Send + Sync {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult;
}
