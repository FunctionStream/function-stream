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

/// Represents a CREATE TABLE or CREATE VIEW statement.
///
/// This wraps the raw SQL AST node so the coordinator pipeline can
/// distinguish table/view creation from other streaming SQL operations.
#[derive(Debug)]
pub struct CreateTable {
    pub statement: DFStatement,
}

impl CreateTable {
    pub fn new(statement: DFStatement) -> Self {
        Self { statement }
    }
}

impl Statement for CreateTable {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_create_table(self, context)
    }

    fn as_create_table(&self) -> Option<&CreateTable> {
        Some(self)
    }
}
