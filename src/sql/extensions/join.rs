use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

use crate::sql::extensions::{NodeWithIncomingEdges, StreamExtension};
use crate::sql::types::StreamSchema;

use std::sync::Arc;
use datafusion_common::plan_err;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use protocol::grpc::api::JoinOperator;
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::FsPhysicalExtensionCodec;
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::common::{FsSchema, FsSchemaRef};

pub(crate) const JOIN_NODE_NAME: &str = "JoinNode";

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

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 2 {
            return plan_err!("join should have exactly two inputs");
        }
        let left_schema = input_schemas[0].clone();
        let right_schema = input_schemas[1].clone();

        let join_plan = planner.sync_plan(&self.rewritten_join)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            join_plan.clone(),
            &FsPhysicalExtensionCodec::default(),
        )?;

        let operator_name = if self.is_instant {
            OperatorName::InstantJoin
        } else {
            OperatorName::Join
        };

        let config = JoinOperator {
            name: format!("join_{index}"),
            left_schema: Some(left_schema.as_ref().clone().into()),
            right_schema: Some(right_schema.as_ref().clone().into()),
            output_schema: Some(self.output_schema().into()),
            join_plan: physical_plan_node.encode_to_vec(),
            ttl_micros: self.ttl.map(|t| t.as_micros() as u64),
        };

        let logical_node = LogicalNode::single(
            index as u32,
            format!("join_{index}"),
            operator_name,
            config.encode_to_vec(),
            "join".to_string(),
            1,
        );

        let left_edge =
            LogicalEdge::project_all(LogicalEdgeType::LeftJoin, left_schema.as_ref().clone());
        let right_edge =
            LogicalEdge::project_all(LogicalEdgeType::RightJoin, right_schema.as_ref().clone());
        Ok(NodeWithIncomingEdges {
            node: logical_node,
            edges: vec![left_edge, right_edge],
        })
    }

    fn output_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(self.schema().inner().clone()).unwrap()
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
