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

use datafusion::common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;

use protocol::grpc::api::ProjectionOperator;

use crate::multifield_partial_ord;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::types::{DFField, schema_from_df_fields};

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const STREAM_PROJECTION_NODE_NAME: &str = "StreamProjectionNode";
const DEFAULT_PROJECTION_LABEL: &str = "projection";

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Projection within a streaming execution topology.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamProjectionNode {
    pub(crate) upstream_plans: Vec<LogicalPlan>,
    pub(crate) operator_label: Option<String>,
    pub(crate) projection_exprs: Vec<Expr>,
    pub(crate) resolved_schema: DFSchemaRef,
    pub(crate) requires_shuffle: bool,
}

multifield_partial_ord!(StreamProjectionNode, operator_label, projection_exprs);

impl StreamProjectionNode {
    pub(crate) fn try_new(
        upstream_plans: Vec<LogicalPlan>,
        operator_label: Option<String>,
        projection_exprs: Vec<Expr>,
    ) -> Result<Self> {
        if upstream_plans.is_empty() {
            return internal_err!("StreamProjectionNode requires at least one upstream plan");
        }
        let primary_input = &upstream_plans[0];
        let upstream_schema = primary_input.schema();

        let mut projected_fields = Vec::with_capacity(projection_exprs.len());
        for logical_expr in &projection_exprs {
            let arrow_field = logical_expr.to_field(upstream_schema)?;
            projected_fields.push(DFField::from(arrow_field));
        }

        let resolved_schema = Arc::new(schema_from_df_fields(&projected_fields)?);

        Ok(Self {
            upstream_plans,
            operator_label,
            projection_exprs,
            resolved_schema,
            requires_shuffle: false,
        })
    }

    pub(crate) fn with_shuffle_routing(mut self) -> Self {
        self.requires_shuffle = true;
        self
    }

    fn validate_uniform_schemas(input_schemas: &[FsSchemaRef]) -> Result<FsSchemaRef> {
        if input_schemas.is_empty() {
            return internal_err!("No input schemas provided to projection planner");
        }
        let primary_schema = input_schemas[0].clone();

        for schema in input_schemas.iter().skip(1) {
            if **schema != *primary_schema {
                return internal_err!(
                    "Schema mismatch: All upstream inputs to a projection node must share the identical schema topology."
                );
            }
        }

        Ok(primary_schema)
    }

    fn compile_physical_expressions(
        &self,
        planner: &Planner,
        input_df_schema: &DFSchemaRef,
    ) -> Result<Vec<Vec<u8>>> {
        self.projection_exprs
            .iter()
            .map(|logical_expr| {
                let physical_expr = planner
                    .create_physical_expr(logical_expr, input_df_schema)
                    .map_err(|e| e.context("Failed to compile physical projection expression"))?;

                let serialized_expr = serialize_physical_expr(
                    &physical_expr,
                    &DefaultPhysicalExtensionCodec {},
                )?;

                Ok(serialized_expr.encode_to_vec())
            })
            .collect()
    }
}

// -----------------------------------------------------------------------------
// Stream Extension Trait Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for StreamProjectionNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        let unified_input_schema = Self::validate_uniform_schemas(&input_schemas)?;
        let input_df_schema =
            Arc::new(DFSchema::try_from(unified_input_schema.schema.as_ref().clone())?);

        let compiled_expr_payloads = self.compile_physical_expressions(planner, &input_df_schema)?;

        let operator_config = ProjectionOperator {
            name: self
                .operator_label
                .as_deref()
                .unwrap_or(DEFAULT_PROJECTION_LABEL)
                .to_string(),
            input_schema: Some(unified_input_schema.as_ref().clone().into()),
            output_schema: Some(self.yielded_schema().into()),
            exprs: compiled_expr_payloads,
        };

        let node_identifier = format!("projection_{node_index}");
        let label = format!(
            "ArrowProjection<{}>",
            self.operator_label.as_deref().unwrap_or("_")
        );

        let logical_node = LogicalNode::single(
            node_index as u32,
            node_identifier,
            OperatorName::Projection,
            operator_config.encode_to_vec(),
            label,
            1,
        );

        let routing_strategy = if self.requires_shuffle {
            LogicalEdgeType::Shuffle
        } else {
            LogicalEdgeType::Forward
        };

        let outgoing_edge =
            LogicalEdge::project_all(routing_strategy, (*unified_input_schema).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![outgoing_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(Arc::new(self.resolved_schema.as_arrow().clone()))
            .expect("Fatal: Failed to generate unkeyed output schema for projection")
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for StreamProjectionNode {
    fn name(&self) -> &str {
        STREAM_PROJECTION_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.upstream_plans.iter().collect()
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
            "StreamProjectionNode: RequiresShuffle={}, Schema={}",
            self.requires_shuffle,
            self.resolved_schema
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let mut new_node = Self::try_new(
            inputs,
            self.operator_label.clone(),
            self.projection_exprs.clone(),
        )?;

        if self.requires_shuffle {
            new_node = new_node.with_shuffle_routing();
        }

        Ok(new_node)
    }
}
