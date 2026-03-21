use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result, TableReference, plan_err};

use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use prost::Message;
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::multifield_partial_ord;
use crate::sql::schema::{ConnectorTable, FieldSpec, Table};
use crate::sql::schema::utils::add_timestamp_field;
use crate::sql::extensions::debezium::DebeziumUnrollingExtension;
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::types::schema_from_df_fields;
use crate::sql::common::{FsSchema, FsSchemaRef, UPDATING_META_FIELD};
use super::{
    StreamExtension, NodeWithIncomingEdges, debezium::ToDebeziumExtension,
    remote_table::RemoteTableExtension,
};
pub(crate) const TABLE_SOURCE_NAME: &str = "TableSourceExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableSourceExtension {
    pub(crate) name: TableReference,
    pub(crate) table: ConnectorTable,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(TableSourceExtension, name, table);

impl TableSourceExtension {
    pub fn new(name: TableReference, table: ConnectorTable) -> Self {
        let physical_fields = table
            .fields
            .iter()
            .filter_map(|field| match field {
                FieldSpec::Struct(field) | FieldSpec::Metadata { field, .. } => {
                    Some((Some(name.clone()), Arc::new(field.clone())).into())
                }
                FieldSpec::Virtual { .. } => None,
            })
            .collect::<Vec<_>>();
        let base_schema = Arc::new(schema_from_df_fields(&physical_fields).unwrap());

        let schema = if table.is_updating() {
            DebeziumUnrollingExtension::as_debezium_schema(&base_schema, Some(name.clone()))
                .unwrap()
        } else {
            base_schema
        };
        let schema = add_timestamp_field(schema, Some(name.clone())).unwrap();
        Self {
            name,
            table,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for TableSourceExtension {
    fn name(&self) -> &str {
        TABLE_SOURCE_NAME
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

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TableSourceExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
        })
    }
}

impl StreamExtension for TableSourceExtension {
    fn node_name(&self) -> Option<NamedNode> {
        Some(NamedNode::Source(self.name.clone()))
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if !input_schemas.is_empty() {
            return plan_err!("TableSourceExtension should not have inputs");
        }
        let sql_source = self.table.as_sql_source()?;
        let node = LogicalNode::single(
            index as u32,
            format!("source_{}_{}", self.name, index),
            OperatorName::ConnectorSource,
            sql_source.source.config.encode_to_vec(),
            sql_source.source.config.description.clone(),
            1,
        );
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![],
        })
    }

    fn output_schema(&self) -> FsSchema {
        FsSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}
