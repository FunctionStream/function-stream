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

use super::{
    CreateFunctionPlan, DropFunctionPlan, ShowFunctionsPlan, StartFunctionPlan, StopFunctionPlan,
};

/// Context passed to PlanVisitor methods
///
/// This context can be extended in the future to include additional information
/// needed by visitors, such as execution environment, configuration, etc.
#[derive(Debug, Clone, Default)]
pub struct PlanVisitorContext {
    // Future: Add fields as needed, e.g.:
    // pub execution_env: Option<ExecutionEnvironment>,
    // pub config: Option<VisitorConfig>,
}

impl PlanVisitorContext {
    pub fn new() -> Self {
        Self::default()
    }
}

use crate::sql::execution::ExecuteError;
use crate::sql::statement::ExecuteResult;

/// Result returned by PlanVisitor methods
///
/// This enum represents all possible return types from PlanVisitor implementations.
/// Different visitors can return different types, which are wrapped in this enum.
#[derive(Debug)]
pub enum PlanVisitorResult {
    /// Execute result (from Executor)
    Execute(Result<ExecuteResult, ExecuteError>),
    // Future: Add more result variants as needed, e.g.:
    // Optimize(BoxedPlanNode),
    // Analyze(Analysis),
}

pub trait PlanVisitor {
    fn visit_create_function(
        &self,
        plan: &CreateFunctionPlan,
        context: &PlanVisitorContext,
    ) -> PlanVisitorResult;

    fn visit_drop_function(
        &self,
        plan: &DropFunctionPlan,
        context: &PlanVisitorContext,
    ) -> PlanVisitorResult;

    fn visit_start_function(
        &self,
        plan: &StartFunctionPlan,
        context: &PlanVisitorContext,
    ) -> PlanVisitorResult;

    fn visit_stop_function(
        &self,
        plan: &StopFunctionPlan,
        context: &PlanVisitorContext,
    ) -> PlanVisitorResult;

    fn visit_show_functions(
        &self,
        plan: &ShowFunctionsPlan,
        context: &PlanVisitorContext,
    ) -> PlanVisitorResult;
}
