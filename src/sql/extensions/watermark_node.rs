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

use datafusion::common::{DFSchemaRef, Result, TableReference, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;
use protocol::grpc::api::ExpressionWatermarkConfig;

use crate::multifield_partial_ord;
use crate::sql::common::constants::{extension_node, runtime_operator_kind};
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::schema::utils::add_timestamp_field;
use crate::sql::types::TIMESTAMP_FIELD;

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const EVENT_TIME_WATERMARK_NODE_NAME: &str = extension_node::EVENT_TIME_WATERMARK;

const DEFAULT_WATERMARK_EMISSION_PERIOD_MICROS: u64 = 1_000_000;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Event-time watermark from a user strategy; drives time progress in stateful operators.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EventTimeWatermarkNode {
    pub(crate) upstream_plan: LogicalPlan,
    pub(crate) namespace_qualifier: TableReference,
    pub(crate) watermark_strategy_expr: Expr,
    pub(crate) resolved_schema: DFSchemaRef,
    pub(crate) internal_timestamp_offset: usize,
}

multifield_partial_ord!(
    EventTimeWatermarkNode,
    upstream_plan,
    namespace_qualifier,
    watermark_strategy_expr,
    internal_timestamp_offset
);

impl EventTimeWatermarkNode {
    pub(crate) fn try_new(
        upstream_plan: LogicalPlan,
        namespace_qualifier: TableReference,
        watermark_strategy_expr: Expr,
    ) -> Result<Self> {
        let resolved_schema = add_timestamp_field(
            upstream_plan.schema().clone(),
            Some(namespace_qualifier.clone()),
        )?;

        let internal_timestamp_offset = resolved_schema
            .index_of_column_by_name(None, TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Fatal: Failed to resolve mandatory temporal column '{}'",
                    TIMESTAMP_FIELD
                ))
            })?;

        Ok(Self {
            upstream_plan,
            namespace_qualifier,
            watermark_strategy_expr,
            resolved_schema,
            internal_timestamp_offset,
        })
    }

    pub(crate) fn generate_fs_schema(&self) -> FsSchema {
        FsSchema::new_unkeyed(
            Arc::new(self.resolved_schema.as_ref().into()),
            self.internal_timestamp_offset,
        )
    }

    fn compile_operator_config(&self, planner: &Planner) -> Result<ExpressionWatermarkConfig> {
        let physical_expr = planner.create_physical_expr(
            &self.watermark_strategy_expr,
            &self.resolved_schema,
        )?;

        let serialized_expr =
            serialize_physical_expr(&physical_expr, &DefaultPhysicalExtensionCodec {})?;

        Ok(ExpressionWatermarkConfig {
            period_micros: DEFAULT_WATERMARK_EMISSION_PERIOD_MICROS,
            idle_time_micros: None,
            expression: serialized_expr.encode_to_vec(),
            input_schema: Some(self.generate_fs_schema().into()),
        })
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for EventTimeWatermarkNode {
    fn name(&self) -> &str {
        EVENT_TIME_WATERMARK_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.watermark_strategy_expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "EventTimeWatermarkNode({}): Schema={}",
            self.namespace_qualifier, self.resolved_schema
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "EventTimeWatermarkNode requires exactly 1 upstream logical plan, but received {}",
                inputs.len()
            );
        }
        if exprs.len() != 1 {
            return internal_err!(
                "EventTimeWatermarkNode requires exactly 1 watermark strategy expression, but received {}",
                exprs.len()
            );
        }

        let internal_timestamp_offset = self
            .resolved_schema
            .index_of_column_by_name(Some(&self.namespace_qualifier), TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Optimizer Error: Lost tracking of temporal column '{}'",
                    TIMESTAMP_FIELD
                ))
            })?;

        Ok(Self {
            upstream_plan: inputs.remove(0),
            namespace_qualifier: self.namespace_qualifier.clone(),
            watermark_strategy_expr: exprs.remove(0),
            resolved_schema: self.resolved_schema.clone(),
            internal_timestamp_offset,
        })
    }
}

// -----------------------------------------------------------------------------
// Core Execution Blueprint Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for EventTimeWatermarkNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        Some(NamedNode::Watermark(self.namespace_qualifier.clone()))
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        mut upstream_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if upstream_schemas.len() != 1 {
            return plan_err!(
                "Topology Violation: EventTimeWatermarkNode requires exactly 1 upstream input, received {}",
                upstream_schemas.len()
            );
        }

        let operator_config = self.compile_operator_config(planner)?;

        let execution_unit = LogicalNode::single(
            node_index as u32,
            format!("watermark_{node_index}"),
            OperatorName::ExpressionWatermark,
            operator_config.encode_to_vec(),
            runtime_operator_kind::WATERMARK_GENERATOR.to_string(),
            1,
        );

        let incoming_edge = LogicalEdge::project_all(
            LogicalEdgeType::Forward,
            (*upstream_schemas.remove(0)).clone(),
        );

        Ok(CompiledTopologyNode {
            execution_unit,
            routing_edges: vec![incoming_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        self.generate_fs_schema()
    }
}
