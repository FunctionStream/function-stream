use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::planner::types::StreamSchema;

pub(crate) const REMOTE_TABLE_NAME: &str = "RemoteTableExtension";

/// Lightweight extension that segments the execution graph and enables merging
/// nodes with the same name. Allows materializing intermediate results.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: TableReference,
    pub(crate) schema: DFSchemaRef,
    pub(crate) materialize: bool,
}

multifield_partial_ord!(RemoteTableExtension, input, name, materialize);

impl StreamExtension for RemoteTableExtension {
    fn node_name(&self) -> Option<NamedNode> {
        if self.materialize {
            Some(NamedNode::RemoteTable(self.name.to_owned()))
        } else {
            None
        }
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for RemoteTableExtension {
    fn name(&self) -> &str {
        REMOTE_TABLE_NAME
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }
        Ok(Self {
            input: inputs[0].clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            materialize: self.materialize,
        })
    }
}
