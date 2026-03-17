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

use std::collections::HashSet;

use crate::datastream::logical::LogicalProgram;

use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

#[derive(Debug)]
pub struct InsertStatementPlan {
    pub program: LogicalProgram,
    pub connection_ids: HashSet<i64>,
}

impl InsertStatementPlan {
    pub fn new(program: LogicalProgram, connection_ids: HashSet<i64>) -> Self {
        Self {
            program,
            connection_ids,
        }
    }
}

impl PlanNode for InsertStatementPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_insert_statement_plan(self, context)
    }
}
