use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::planner::schemas::add_timestamp_field;
use crate::sql::planner::types::{StreamSchema, TIMESTAMP_FIELD};

pub(crate) const WATERMARK_NODE_NAME: &str = "WatermarkNode";

/// Represents a watermark node in the streaming query plan.
/// Watermarks track event-time progress and enable time-based operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatermarkNode {
    pub input: LogicalPlan,
    pub qualifier: TableReference,
    pub watermark_expression: Expr,
    pub schema: DFSchemaRef,
    timestamp_index: usize,
}

multifield_partial_ord!(
    WatermarkNode,
    input,
    qualifier,
    watermark_expression,
    timestamp_index
);

impl UserDefinedLogicalNodeCore for WatermarkNode {
    fn name(&self) -> &str {
        WATERMARK_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.watermark_expression.clone()]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WatermarkNode({}): {}", self.qualifier, self.schema)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }
        if exprs.len() != 1 {
            return internal_err!("expected one expression; found {}", exprs.len());
        }

        let timestamp_index = self
            .schema
            .index_of_column_by_name(Some(&self.qualifier), TIMESTAMP_FIELD)
            .ok_or_else(|| DataFusionError::Plan("missing timestamp column".to_string()))?;

        Ok(Self {
            input: inputs[0].clone(),
            qualifier: self.qualifier.clone(),
            watermark_expression: exprs.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            timestamp_index,
        })
    }
}

impl StreamExtension for WatermarkNode {
    fn node_name(&self) -> Option<NamedNode> {
        Some(NamedNode::Watermark(self.qualifier.clone()))
    }

    fn output_schema(&self) -> StreamSchema {
        self.stream_schema()
    }
}

impl WatermarkNode {
    pub(crate) fn new(
        input: LogicalPlan,
        qualifier: TableReference,
        watermark_expression: Expr,
    ) -> Result<Self> {
        let schema = add_timestamp_field(input.schema().clone(), Some(qualifier.clone()))?;
        let timestamp_index = schema
            .index_of_column_by_name(None, TIMESTAMP_FIELD)
            .ok_or_else(|| DataFusionError::Plan("missing _timestamp column".to_string()))?;
        Ok(Self {
            input,
            qualifier,
            watermark_expression,
            schema,
            timestamp_index,
        })
    }

    pub(crate) fn stream_schema(&self) -> StreamSchema {
        StreamSchema::new_unkeyed(Arc::new(self.schema.as_ref().into()), self.timestamp_index)
    }
}
