use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::types::StreamSchema;

use std::sync::Arc;

pub(crate) const JOIN_NODE_NAME: &str = "JoinNode";

/// Extension node for streaming joins.
/// Supports instant joins (windowed, no state) and updating joins (with TTL-based state).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct JoinExtension {
    pub(crate) rewritten_join: LogicalPlan,
    pub(crate) is_instant: bool,
    pub(crate) ttl: Option<Duration>,
}

impl StreamExtension for JoinExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into())).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for JoinExtension {
    fn name(&self) -> &str {
        JOIN_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.rewritten_join]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.rewritten_join.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "JoinExtension: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            rewritten_join: inputs[0].clone(),
            is_instant: self.is_instant,
            ttl: self.ttl,
        })
    }
}
