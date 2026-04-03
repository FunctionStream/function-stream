// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Formatter;
use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::plan_err;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use protocol::grpc::api::JoinOperator;

use crate::sql::common::constants::{extension_node, runtime_operator_kind};
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::physical::FsPhysicalExtensionCodec;

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

pub(crate) const STREAM_JOIN_NODE_TYPE: &str = extension_node::STREAMING_JOIN;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// A logical plan node representing a streaming join operation.
/// It bridges the DataFusion logical plan with the physical streaming execution engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct StreamingJoinNode {
    pub(crate) underlying_plan: LogicalPlan,
    pub(crate) instant_execution_mode: bool,
    pub(crate) state_retention_ttl: Option<Duration>,
}

impl StreamingJoinNode {
    /// Creates a new instance of the streaming join node.
    pub fn new(
        underlying_plan: LogicalPlan,
        instant_execution_mode: bool,
        state_retention_ttl: Option<Duration>,
    ) -> Self {
        Self {
            underlying_plan,
            instant_execution_mode,
            state_retention_ttl,
        }
    }

    /// Compiles the physical execution plan and serializes it into a Protobuf configuration payload.
    fn compile_operator_config(
        &self,
        planner: &Planner,
        node_identifier: &str,
        left_schema: FsSchemaRef,
        right_schema: FsSchemaRef,
    ) -> Result<JoinOperator> {
        let physical_plan = planner.sync_plan(&self.underlying_plan)?;

        let proto_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &FsPhysicalExtensionCodec::default(),
        )?;

        Ok(JoinOperator {
            name: node_identifier.to_string(),
            left_schema: Some(left_schema.as_ref().clone().into()),
            right_schema: Some(right_schema.as_ref().clone().into()),
            output_schema: Some(self.extract_fs_schema().into()),
            join_plan: proto_node.encode_to_vec(),
            ttl_micros: self.state_retention_ttl.map(|ttl| ttl.as_micros() as u64),
        })
    }

    fn determine_operator_type(&self) -> OperatorName {
        if self.instant_execution_mode {
            OperatorName::InstantJoin
        } else {
            OperatorName::Join
        }
    }

    fn extract_fs_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(self.underlying_plan.schema().inner().clone())
            .expect("Fatal: Failed to convert internal join schema to FsSchema without keys")
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Core Implementation
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for StreamingJoinNode {
    fn name(&self) -> &str {
        STREAM_JOIN_NODE_TYPE
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.underlying_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.underlying_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamingJoinNode: Schema={}, InstantMode={}, TTL={:?}",
            self.schema(),
            self.instant_execution_mode,
            self.state_retention_ttl
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "StreamingJoinNode expects exactly 1 underlying logical plan during recreation"
            );
        }

        Ok(Self::new(
            inputs.remove(0),
            self.instant_execution_mode,
            self.state_retention_ttl,
        ))
    }
}

// -----------------------------------------------------------------------------
// Streaming Graph Extension Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for StreamingJoinNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        mut input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if input_schemas.len() != 2 {
            return plan_err!(
                "Invalid topology: StreamingJoinNode requires exactly two upstream inputs, received {}",
                input_schemas.len()
            );
        }

        let right_schema = input_schemas.pop().unwrap();
        let left_schema = input_schemas.pop().unwrap();

        let node_identifier = format!("stream_join_{node_index}");

        let operator_config = self.compile_operator_config(
            planner,
            &node_identifier,
            left_schema.clone(),
            right_schema.clone(),
        )?;

        let logical_node = LogicalNode::single(
            node_index as u32,
            node_identifier.clone(),
            self.determine_operator_type(),
            operator_config.encode_to_vec(),
            runtime_operator_kind::STREAMING_JOIN.to_string(),
            1,
        );

        let left_edge =
            LogicalEdge::project_all(LogicalEdgeType::LeftJoin, left_schema.as_ref().clone());
        let right_edge =
            LogicalEdge::project_all(LogicalEdgeType::RightJoin, right_schema.as_ref().clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![left_edge, right_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        self.extract_fs_schema()
    }
}
