use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::planner::types::StreamSchema;

pub(crate) const WINDOW_FUNCTION_EXTENSION_NAME: &str = "WindowFunctionExtension";

/// Extension for window functions (e.g., ROW_NUMBER, RANK) over windowed input.
/// Window functions require already-windowed input and are evaluated per-window.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct WindowFunctionExtension {
    pub(crate) window_plan: LogicalPlan,
    pub(crate) key_fields: Vec<usize>,
}

impl WindowFunctionExtension {
    pub fn new(window_plan: LogicalPlan, key_fields: Vec<usize>) -> Self {
        Self {
            window_plan,
            key_fields,
        }
    }
}

impl UserDefinedLogicalNodeCore for WindowFunctionExtension {
    fn name(&self) -> &str {
        WINDOW_FUNCTION_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.window_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.window_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WindowFunction: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self::new(inputs[0].clone(), self.key_fields.clone()))
    }
}

impl StreamExtension for WindowFunctionExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().clone().into())).unwrap()
    }
}
