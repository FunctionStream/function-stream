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
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;

use protocol::function_stream_graph::ValuePlanOperator;

use crate::multifield_partial_ord;
use crate::sql::common::constants::extension_node;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_node::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::physical::StreamingExtensionCodec;

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const REMOTE_TABLE_NODE_NAME: &str = extension_node::REMOTE_TABLE_BOUNDARY;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Segments the execution graph and merges nodes sharing the same identifier; acts as a boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableBoundaryNode {
    pub(crate) upstream_plan: LogicalPlan,
    pub(crate) table_identifier: TableReference,
    pub(crate) resolved_schema: DFSchemaRef,
    pub(crate) requires_materialization: bool,
}

multifield_partial_ord!(
    RemoteTableBoundaryNode,
    upstream_plan,
    table_identifier,
    requires_materialization
);

impl RemoteTableBoundaryNode {
    fn compile_engine_operator(&self, planner: &Planner) -> Result<Vec<u8>> {
        let physical_plan = planner.sync_plan(&self.upstream_plan)?;

        let physical_plan_proto = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &StreamingExtensionCodec::default(),
        )?;

        let operator_config = ValuePlanOperator {
            name: format!("value_calculation({})", self.table_identifier),
            physical_plan: physical_plan_proto.encode_to_vec(),
        };

        Ok(operator_config.encode_to_vec())
    }

    fn validate_uniform_schemas(input_schemas: &[FsSchemaRef]) -> Result<()> {
        if input_schemas.len() <= 1 {
            return Ok(());
        }

        let primary_schema = &input_schemas[0];
        for schema in input_schemas.iter().skip(1) {
            if *schema != *primary_schema {
                return plan_err!(
                    "Topology error: Multiple input streams routed to the same remote table must share an identical schema structure."
                );
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Stream Extension Trait Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for RemoteTableBoundaryNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        if self.requires_materialization {
            Some(NamedNode::RemoteTable(self.table_identifier.clone()))
        } else {
            None
        }
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        Self::validate_uniform_schemas(&input_schemas)?;

        let operator_payload = self.compile_engine_operator(planner)?;

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("value_{node_index}"),
            OperatorName::Value,
            operator_payload,
            self.table_identifier.to_string(),
            planner.default_parallelism(),
        );

        let routing_edges: Vec<LogicalEdge> = input_schemas
            .into_iter()
            .map(|schema| LogicalEdge::project_all(LogicalEdgeType::Forward, (*schema).clone()))
            .collect();

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges,
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_keys(Arc::new(self.resolved_schema.as_ref().into()), vec![])
            .expect("Fatal: Failed to generate output schema for remote table boundary")
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for RemoteTableBoundaryNode {
    fn name(&self) -> &str {
        REMOTE_TABLE_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteTableBoundaryNode: Identifier={}, Materialized={}, Schema={}",
            self.table_identifier, self.requires_materialization, self.resolved_schema
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "RemoteTableBoundaryNode expects exactly 1 upstream logical plan, but received {}",
                inputs.len()
            );
        }

        Ok(Self {
            upstream_plan: inputs.remove(0),
            table_identifier: self.table_identifier.clone(),
            resolved_schema: self.resolved_schema.clone(),
            requires_materialization: self.requires_materialization,
        })
    }
}
