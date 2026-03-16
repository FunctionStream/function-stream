use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::types::{DFField, StreamSchema, schema_from_df_fields};

pub(crate) const PROJECTION_NAME: &str = "ProjectionExtension";

/// Projection operations for streaming SQL plans.
/// Handles column projections, shuffles for key-based operations, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionExtension {
    pub(crate) inputs: Vec<LogicalPlan>,
    pub(crate) name: Option<String>,
    pub(crate) exprs: Vec<Expr>,
    pub(crate) schema: DFSchemaRef,
    pub(crate) shuffle: bool,
}

multifield_partial_ord!(ProjectionExtension, name, exprs);

impl ProjectionExtension {
    pub(crate) fn new(inputs: Vec<LogicalPlan>, name: Option<String>, exprs: Vec<Expr>) -> Self {
        let input_schema = inputs.first().unwrap().schema();
        let fields: Vec<DFField> = exprs
            .iter()
            .map(|e| DFField::from(e.to_field(input_schema).unwrap()))
            .collect();

        let schema = Arc::new(schema_from_df_fields(&fields).unwrap());

        Self {
            inputs,
            name,
            exprs,
            schema,
            shuffle: false,
        }
    }

    pub(crate) fn shuffled(mut self) -> Self {
        self.shuffle = true;
        self
    }
}

impl StreamExtension for ProjectionExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema.as_arrow().clone())).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for ProjectionExtension {
    fn name(&self) -> &str {
        PROJECTION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ProjectionExtension: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            inputs,
            exprs,
            schema: self.schema.clone(),
            shuffle: self.shuffle,
        })
    }
}
