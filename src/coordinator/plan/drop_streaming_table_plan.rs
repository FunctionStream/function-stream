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

#[derive(Debug, Clone)]
pub struct DropStreamingTablePlan {
    pub table_name: String,
    pub if_exists: bool,
}

impl DropStreamingTablePlan {
    pub fn new(table_name: String, if_exists: bool) -> Self {
        Self {
            table_name,
            if_exists,
        }
    }
}

impl PlanNode for DropStreamingTablePlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_drop_streaming_table(self, context)
    }
}
