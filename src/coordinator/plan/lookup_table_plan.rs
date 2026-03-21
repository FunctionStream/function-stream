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

use crate::sql::schema::source_table::SourceTable;

use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

/// Plan node that exposes a lookup table config as a logical plan input.
#[derive(Debug)]
pub struct LookupTablePlan {
    pub table: SourceTable,
}

impl PlanNode for LookupTablePlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_lookup_table(self, context)
    }
}
