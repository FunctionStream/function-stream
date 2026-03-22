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

use datafusion::logical_expr::LogicalPlan;

use crate::sql::schema::SourceTable;

use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};

/// Payload for [`CreateTablePlan`]: either a DataFusion DDL plan or a connector `CREATE TABLE` (no `AS SELECT`).
#[derive(Debug, Clone)]
pub enum CreateTablePlanBody {
    DataFusion(LogicalPlan),
    ConnectorSource {
        source_table: SourceTable,
        if_not_exists: bool,
    },
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub body: CreateTablePlanBody,
}

impl CreateTablePlan {
    pub fn new(logical_plan: LogicalPlan) -> Self {
        Self {
            body: CreateTablePlanBody::DataFusion(logical_plan),
        }
    }

    pub fn connector_source(source_table: SourceTable, if_not_exists: bool) -> Self {
        Self {
            body: CreateTablePlanBody::ConnectorSource {
                source_table,
                if_not_exists,
            },
        }
    }
}

impl PlanNode for CreateTablePlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_create_table_plan(self, context)
    }
}
