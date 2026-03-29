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

use std::fmt::Formatter;

use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::common::constants::extension_node;
use crate::sql::schema::utils::{add_timestamp_field, has_timestamp_field};

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const TIMESTAMP_INJECTOR_NODE_NAME: &str = extension_node::SYSTEM_TIMESTAMP_INJECTOR;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Injects the mandatory system `_timestamp` field into the upstream streaming schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SystemTimestampInjectorNode {
    pub(crate) upstream_plan: LogicalPlan,
    pub(crate) target_qualifier: Option<TableReference>,
    pub(crate) resolved_schema: DFSchemaRef,
}

multifield_partial_ord!(SystemTimestampInjectorNode, upstream_plan, target_qualifier);

impl SystemTimestampInjectorNode {
    pub(crate) fn try_new(
        upstream_plan: LogicalPlan,
        target_qualifier: Option<TableReference>,
    ) -> Result<Self> {
        let upstream_schema = upstream_plan.schema();

        if has_timestamp_field(upstream_schema) {
            return internal_err!(
                "Topology Violation: Attempted to inject a system timestamp into an upstream plan \
                 that already contains one. \
                 \nPlan:\n {:?} \nSchema:\n {:?}",
                upstream_plan,
                upstream_schema
            );
        }

        let resolved_schema =
            add_timestamp_field(upstream_schema.clone(), target_qualifier.clone())?;

        Ok(Self {
            upstream_plan,
            target_qualifier,
            resolved_schema,
        })
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for SystemTimestampInjectorNode {
    fn name(&self) -> &str {
        TIMESTAMP_INJECTOR_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        let field_names = self
            .resolved_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "SystemTimestampInjector(Qualifier={:?}): [{}]",
            self.target_qualifier, field_names
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "SystemTimestampInjectorNode requires exactly 1 upstream logical plan, but received {}",
                inputs.len()
            );
        }

        Self::try_new(inputs.remove(0), self.target_qualifier.clone())
    }
}
