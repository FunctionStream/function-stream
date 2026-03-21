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

use datafusion::common::{DFSchemaRef, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::schema::utils::{add_timestamp_field, has_timestamp_field};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TimestampAppendExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) qualifier: Option<TableReference>,
    pub(crate) schema: DFSchemaRef,
}

impl TimestampAppendExtension {
    pub(crate) fn new(input: LogicalPlan, qualifier: Option<TableReference>) -> Self {
        if has_timestamp_field(input.schema()) {
            unreachable!(
                "shouldn't be adding timestamp to a plan that already has it: plan :\n {:?}\n schema: {:?}",
                input,
                input.schema()
            );
        }
        let schema = add_timestamp_field(input.schema().clone(), qualifier.clone()).unwrap();
        Self {
            input,
            qualifier,
            schema,
        }
    }
}

multifield_partial_ord!(TimestampAppendExtension, input, qualifier);

impl UserDefinedLogicalNodeCore for TimestampAppendExtension {
    fn name(&self) -> &str {
        "TimestampAppendExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TimestampAppendExtension({:?}): {}",
            self.qualifier,
            self.schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self::new(inputs[0].clone(), self.qualifier.clone()))
    }
}
