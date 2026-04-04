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
use std::sync::Arc;

use datafusion::common::{Column, DFSchema, DFSchemaRef, Result, internal_err, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use protocol::grpc::api::WindowFunctionOperator;

use crate::sql::common::constants::{extension_node, proto_operator_name, runtime_operator_kind};
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::physical::StreamingExtensionCodec;
use crate::sql::types::TIMESTAMP_FIELD;

use super::{CompiledTopologyNode, StreamingOperatorBlueprint};

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const STREAMING_WINDOW_NODE_NAME: &str = extension_node::STREAMING_WINDOW_FUNCTION;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Stateful streaming window: temporal binning plus underlying window evaluation plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct StreamingWindowFunctionNode {
    pub(crate) underlying_evaluation_plan: LogicalPlan,
    pub(crate) partition_key_indices: Vec<usize>,
}

impl StreamingWindowFunctionNode {
    pub fn new(underlying_evaluation_plan: LogicalPlan, partition_key_indices: Vec<usize>) -> Self {
        Self {
            underlying_evaluation_plan,
            partition_key_indices,
        }
    }

    fn compile_temporal_binning_function(
        &self,
        planner: &Planner,
        input_df_schema: &DFSchema,
    ) -> Result<Vec<u8>> {
        let timestamp_column = Expr::Column(Column::new_unqualified(TIMESTAMP_FIELD.to_string()));

        let physical_binning_expr =
            planner.create_physical_expr(&timestamp_column, input_df_schema)?;

        let serialized_expr =
            serialize_physical_expr(&physical_binning_expr, &DefaultPhysicalExtensionCodec {})?;

        Ok(serialized_expr.encode_to_vec())
    }

    fn compile_physical_evaluation_plan(&self, planner: &Planner) -> Result<Vec<u8>> {
        let physical_window_plan = planner.sync_plan(&self.underlying_evaluation_plan)?;

        let proto_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_window_plan,
            &StreamingExtensionCodec::default(),
        )?;

        Ok(proto_plan_node.encode_to_vec())
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for StreamingWindowFunctionNode {
    fn name(&self) -> &str {
        STREAMING_WINDOW_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.underlying_evaluation_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.underlying_evaluation_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamingWindowFunction: Schema={}", self.schema())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "StreamingWindowFunctionNode requires exactly 1 upstream input, got {}",
                inputs.len()
            );
        }

        Ok(Self::new(
            inputs.remove(0),
            self.partition_key_indices.clone(),
        ))
    }
}

// -----------------------------------------------------------------------------
// Core Execution Blueprint Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for StreamingWindowFunctionNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        mut input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if input_schemas.len() != 1 {
            return plan_err!(
                "Topology Violation: StreamingWindowFunctionNode requires exactly 1 upstream input schema, received {}",
                input_schemas.len()
            );
        }

        let input_schema = input_schemas.remove(0);

        let input_df_schema = DFSchema::try_from(input_schema.schema.as_ref().clone())?;

        let binning_payload = self.compile_temporal_binning_function(planner, &input_df_schema)?;
        let evaluation_plan_payload = self.compile_physical_evaluation_plan(planner)?;

        let operator_config = WindowFunctionOperator {
            name: proto_operator_name::WINDOW_FUNCTION.to_string(),
            input_schema: Some(input_schema.as_ref().clone().into()),
            binning_function: binning_payload,
            window_function_plan: evaluation_plan_payload,
        };

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("window_function_{node_index}"),
            OperatorName::WindowFunction,
            operator_config.encode_to_vec(),
            runtime_operator_kind::STREAMING_WINDOW_EVALUATOR.to_string(),
            1,
        );

        let routing_edge =
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*input_schema).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![routing_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().clone().into())).expect(
            "Fatal: Failed to generate unkeyed output schema for StreamingWindowFunctionNode",
        )
    }
}
