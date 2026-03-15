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

use datafusion::sql::sqlparser::ast::Statement as DFStatement;

use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

/// Wraps a DataFusion SQL statement (SELECT, INSERT, CREATE TABLE, etc.)
/// so it can flow through the same Statement → StatementVisitor pipeline
/// as FunctionStream DDL commands.
#[derive(Debug)]
pub struct StreamingSql {
    pub statement: DFStatement,
}

impl StreamingSql {
    pub fn new(statement: DFStatement) -> Self {
        Self { statement }
    }
}

impl Statement for StreamingSql {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_streaming_sql(self, context)
    }
}
