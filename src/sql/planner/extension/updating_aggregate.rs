use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use super::{IsRetractExtension, NamedNode, StreamExtension};
use crate::sql::types::StreamSchema;

pub(crate) const UPDATING_AGGREGATE_EXTENSION_NAME: &str = "UpdatingAggregateExtension";

/// Extension node for updating (non-windowed) aggregations.
/// Maintains state with TTL and emits retraction/update pairs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct UpdatingAggregateExtension {
    pub(crate) aggregate: LogicalPlan,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
    pub(crate) timestamp_qualifier: Option<TableReference>,
    pub(crate) ttl: Duration,
}

impl UpdatingAggregateExtension {
    pub fn new(
        aggregate: LogicalPlan,
        key_fields: Vec<usize>,
        timestamp_qualifier: Option<TableReference>,
        ttl: Duration,
    ) -> Result<Self> {
        let final_calculation = LogicalPlan::Extension(Extension {
            node: Arc::new(IsRetractExtension::new(
                aggregate.clone(),
                timestamp_qualifier.clone(),
            )),
        });

        Ok(Self {
            aggregate,
            key_fields,
            final_calculation,
            timestamp_qualifier,
            ttl,
        })
    }
}

impl UserDefinedLogicalNodeCore for UpdatingAggregateExtension {
    fn name(&self) -> &str {
        UPDATING_AGGREGATE_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.aggregate]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.final_calculation.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UpdatingAggregateExtension")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!("UpdatingAggregateExtension expects exactly one input");
        }
        Self::new(
            inputs[0].clone(),
            self.key_fields.clone(),
            self.timestamp_qualifier.clone(),
            self.ttl,
        )
    }
}

impl StreamExtension for UpdatingAggregateExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into())).unwrap()
    }
}
