use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{Column, DFSchemaRef, JoinType, Result, TableReference, internal_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use super::{NamedNode, StreamExtension};
use crate::multifield_partial_ord;
use crate::sql::catalog::connector_table::ConnectorTable;
use crate::sql::types::StreamSchema;

pub const SOURCE_EXTENSION_NAME: &str = "LookupSource";
pub const JOIN_EXTENSION_NAME: &str = "LookupJoin";

/// Represents a lookup table source in the streaming plan.
/// Lookup sources provide point-query access to external state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupSource {
    pub(crate) table: ConnectorTable,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(LookupSource, table);

impl UserDefinedLogicalNodeCore for LookupSource {
    fn name(&self) -> &str {
        SOURCE_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LookupSource: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return internal_err!("LookupSource cannot have inputs");
        }
        Ok(Self {
            table: self.table.clone(),
            schema: self.schema.clone(),
        })
    }
}

impl StreamExtension for LookupSource {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }
}

/// Represents a lookup join: a streaming input joined against a lookup table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupJoin {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) connector: ConnectorTable,
    pub(crate) on: Vec<(Expr, Column)>,
    pub(crate) filter: Option<Expr>,
    pub(crate) alias: Option<TableReference>,
    pub(crate) join_type: JoinType,
}

multifield_partial_ord!(LookupJoin, input, connector, on, filter, alias);

impl UserDefinedLogicalNodeCore for LookupJoin {
    fn name(&self) -> &str {
        JOIN_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut e: Vec<_> = self.on.iter().map(|(l, _)| l.clone()).collect();
        if let Some(filter) = &self.filter {
            e.push(filter.clone());
        }
        e
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LookupJoinExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            input: inputs[0].clone(),
            schema: self.schema.clone(),
            connector: self.connector.clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            alias: self.alias.clone(),
            join_type: self.join_type,
        })
    }
}

impl StreamExtension for LookupJoin {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }
}
