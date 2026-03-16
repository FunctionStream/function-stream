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

use datafusion::common::Result;
use datafusion::logical_expr::{DmlStatement, LogicalPlan, WriteOp};
use datafusion::sql::sqlparser::ast::Statement;

use super::optimizer::produce_optimized_plan;
use crate::sql::planner::StreamSchemaProvider;

/// Represents an INSERT operation in a streaming SQL pipeline.
#[derive(Debug)]
pub enum Insert {
    /// Insert into a named sink table.
    InsertQuery {
        sink_name: String,
        logical_plan: LogicalPlan,
    },
    /// An anonymous query (no explicit INSERT target).
    Anonymous { logical_plan: LogicalPlan },
}

impl Insert {
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &StreamSchemaProvider,
    ) -> Result<Insert> {
        let logical_plan = produce_optimized_plan(statement, schema_provider)?;

        match &logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Insert(_),
                input,
                ..
            }) => {
                let sink_name = table_name.to_string();
                Ok(Insert::InsertQuery {
                    sink_name,
                    logical_plan: (**input).clone(),
                })
            }
            _ => Ok(Insert::Anonymous { logical_plan }),
        }
    }
}
