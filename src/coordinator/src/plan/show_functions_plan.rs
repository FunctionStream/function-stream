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

use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug, Clone, Default)]
pub struct ShowFunctionsPlan {}

impl ShowFunctionsPlan {
    pub fn new() -> Self {
        Self {}
    }
}

impl PlanNode for ShowFunctionsPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_show_functions(self, context)
    }
}
