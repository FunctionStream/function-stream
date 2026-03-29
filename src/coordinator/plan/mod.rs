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
mod create_python_function_plan;
mod create_table_plan;
mod drop_function_plan;
mod drop_table_plan;
mod logical_plan_visitor;
mod lookup_table_plan;
mod optimizer;
mod show_catalog_tables_plan;
mod show_create_table_plan;
mod show_functions_plan;
mod start_function_plan;
mod stop_function_plan;
mod streaming_table_connector_plan;
mod streaming_table_plan;
mod visitor;

pub use create_function_plan::CreateFunctionPlan;
pub use create_python_function_plan::CreatePythonFunctionPlan;
pub use create_table_plan::{CreateTablePlan, CreateTablePlanBody};
pub use drop_function_plan::DropFunctionPlan;
pub use drop_table_plan::DropTablePlan;
pub use logical_plan_visitor::LogicalPlanVisitor;
pub use lookup_table_plan::LookupTablePlan;
pub use optimizer::LogicalPlanner;
pub use show_catalog_tables_plan::ShowCatalogTablesPlan;
pub use show_create_table_plan::ShowCreateTablePlan;
pub use show_functions_plan::ShowFunctionsPlan;
pub use start_function_plan::StartFunctionPlan;
pub use stop_function_plan::StopFunctionPlan;
pub use streaming_table_connector_plan::StreamingTableConnectorPlan;
pub use streaming_table_plan::StreamingTable;
pub use visitor::{PlanVisitor, PlanVisitorContext, PlanVisitorResult};

use std::fmt;

pub trait PlanNode: fmt::Debug + Send + Sync {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult;
}
