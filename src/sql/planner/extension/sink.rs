use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use super::debezium::ToDebeziumExtension;
use super::remote_table::RemoteTableExtension;
use super::{NamedNode, StreamExtension};
use crate::multifield_partial_ord;
use crate::sql::catalog::table::Table;
use crate::sql::types::StreamSchema;

pub(crate) const SINK_NODE_NAME: &str = "SinkExtension";

/// Extension node representing a sink (output) in the streaming plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SinkExtension {
    pub(crate) name: TableReference,
    pub(crate) table: Table,
    pub(crate) schema: DFSchemaRef,
    pub(crate) inputs: Arc<Vec<LogicalPlan>>,
}

multifield_partial_ord!(SinkExtension, name, inputs);

impl SinkExtension {
    pub fn new(
        name: TableReference,
        table: Table,
        mut schema: DFSchemaRef,
        mut input: Arc<LogicalPlan>,
    ) -> Result<Self> {
        match &table {
            Table::ConnectorTable(connector_table) => {
                if connector_table.is_updating() {
                    let to_debezium = ToDebeziumExtension::try_new(input.as_ref().clone())?;
                    input = Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(to_debezium),
                    }));
                    schema = input.schema().clone();
                }
            }
            Table::LookupTable(..) => return plan_err!("cannot use a lookup table as a sink"),
            Table::TableFromQuery { .. } => {}
        }

        Self::add_remote_if_necessary(&schema, &mut input);

        let inputs = Arc::new(vec![(*input).clone()]);
        Ok(Self {
            name,
            table,
            schema,
            inputs,
        })
    }

    pub fn add_remote_if_necessary(schema: &DFSchemaRef, input: &mut Arc<LogicalPlan>) {
        if let LogicalPlan::Extension(node) = input.as_ref() {
            let Ok(ext): Result<&dyn StreamExtension, _> = (&node.node).try_into() else {
                // not a StreamExtension, wrap it
                let remote = RemoteTableExtension {
                    input: input.as_ref().clone(),
                    name: TableReference::bare("sink projection"),
                    schema: schema.clone(),
                    materialize: false,
                };
                *input = Arc::new(LogicalPlan::Extension(Extension {
                    node: Arc::new(remote),
                }));
                return;
            };
            if !ext.transparent() {
                return;
            }
        }
        let remote = RemoteTableExtension {
            input: input.as_ref().clone(),
            name: TableReference::bare("sink projection"),
            schema: schema.clone(),
            materialize: false,
        };
        *input = Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(remote),
        }));
    }
}

impl UserDefinedLogicalNodeCore for SinkExtension {
    fn name(&self) -> &str {
        SINK_NODE_NAME
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

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SinkExtension({:?}): {}", self.name, self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
            inputs: Arc::new(inputs),
        })
    }
}

impl StreamExtension for SinkExtension {
    fn node_name(&self) -> Option<NamedNode> {
        Some(NamedNode::Sink(self.name.clone()))
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_fields(vec![])
    }
}
