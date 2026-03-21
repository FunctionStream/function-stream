use std::{fmt::Formatter, sync::Arc};

use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err, plan_err};

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use protocol::grpc::api::ValuePlanOperator;
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::multifield_partial_ord;
use crate::sql::logical_planner::FsPhysicalExtensionCodec;
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::common::{FsSchema, FsSchemaRef};
use super::{StreamExtension, NodeWithIncomingEdges};

pub(crate) const REMOTE_TABLE_NAME: &str = "RemoteTableExtension";

/* Lightweight extension that allows us to segment the graph and merge nodes with the same name.
  An Extension Planner will be used to isolate computation to individual nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: TableReference,
    pub(crate) schema: DFSchemaRef,
    pub(crate) materialize: bool,
}

multifield_partial_ord!(RemoteTableExtension, input, name, materialize);

impl RemoteTableExtension {
    fn plan_node_inlined(
        planner: &Planner,
        index: usize,
        this: &RemoteTableExtension,
    ) -> Result<NodeWithIncomingEdges> {
        let physical_plan = planner.sync_plan(&this.input)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &FsPhysicalExtensionCodec::default(),
        )?;
        let config = ValuePlanOperator {
            name: format!("value_calculation({})", this.name),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node = LogicalNode::single(
            index as u32,
            format!("value_{index}"),
            OperatorName::ArrowValue,
            config.encode_to_vec(),
            this.name.to_string(),
            1,
        );
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![],
        })
    }

    fn plan_node_with_edges(
        planner: &Planner,
        index: usize,
        this: &RemoteTableExtension,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        let physical_plan = planner.sync_plan(&this.input)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &FsPhysicalExtensionCodec::default(),
        )?;
        let config = ValuePlanOperator {
            name: format!("value_calculation({})", this.name),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node = LogicalNode::single(
            index as u32,
            format!("value_{index}"),
            OperatorName::ArrowValue,
            config.encode_to_vec(),
            this.name.to_string(),
            1,
        );

        let edges = input_schemas
            .into_iter()
            .map(|schema| LogicalEdge::project_all(LogicalEdgeType::Forward, (*schema).clone()))
            .collect();
        Ok(NodeWithIncomingEdges { node, edges })
    }
}

impl StreamExtension for RemoteTableExtension {
    fn node_name(&self) -> Option<NamedNode> {
        if self.materialize {
            Some(NamedNode::RemoteTable(self.name.to_owned()))
        } else {
            None
        }
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        match input_schemas.len() {
            0 => {
                return Self::plan_node_inlined(planner, index, self);
            }
            1 => {}
            _multiple_inputs => {
                let first = input_schemas[0].clone();
                for schema in input_schemas.iter().skip(1) {
                    if *schema != first {
                        return plan_err!(
                            "If a node has multiple inputs, they must all have the same schema"
                        );
                    }
                }
            }
        }
        Self::plan_node_with_edges(planner, index, self, input_schemas)
    }

    fn output_schema(&self) -> FsSchema {
        FsSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
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
