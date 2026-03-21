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

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFSchemaRef, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::logical_planner::updating_meta_field;
use crate::sql::types::{DFField, TIMESTAMP_FIELD, fields_with_qualifiers, schema_from_df_fields};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IsRetractExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) timestamp_qualifier: Option<TableReference>,
}

multifield_partial_ord!(IsRetractExtension, input, timestamp_qualifier);

impl IsRetractExtension {
    pub(crate) fn new(input: LogicalPlan, timestamp_qualifier: Option<TableReference>) -> Self {
        let mut output_fields = fields_with_qualifiers(input.schema());

        let timestamp_index = output_fields.len() - 1;
        output_fields[timestamp_index] = DFField::new(
            timestamp_qualifier.clone(),
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        output_fields.push((timestamp_qualifier.clone(), updating_meta_field()).into());
        let schema = Arc::new(schema_from_df_fields(&output_fields).unwrap());
        Self {
            input,
            schema,
            timestamp_qualifier,
        }
    }
}

impl UserDefinedLogicalNodeCore for IsRetractExtension {
    fn name(&self) -> &str {
        "IsRetractExtension"
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
        write!(f, "IsRetractExtension")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self::new(
            inputs[0].clone(),
            self.timestamp_qualifier.clone(),
        ))
    }
}
