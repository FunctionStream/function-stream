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

use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result, TableReference, ToDFSchema, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore, col, lit,
};
use datafusion::prelude::named_struct;
use datafusion::scalar::ScalarValue;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use protocol::grpc::api::UpdatingAggregateOperator;

use crate::sql::common::constants::{extension_node, proto_operator_name, updating_state_field};
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{
    CompiledTopologyNode, IsRetractExtension, StreamingOperatorBlueprint,
};
use crate::sql::functions::multi_hash;
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::physical::FsPhysicalExtensionCodec;

// -----------------------------------------------------------------------------
// Constants & Configuration
// -----------------------------------------------------------------------------

pub(crate) const CONTINUOUS_AGGREGATE_NODE_NAME: &str = extension_node::CONTINUOUS_AGGREGATE;

const DEFAULT_FLUSH_INTERVAL_MICROS: u64 = 10_000_000;

const STATIC_HASH_SIZE_BYTES: i32 = 16;

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Stateful continuous aggregation: running aggregates with updating / retraction semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct ContinuousAggregateNode {
    pub(crate) base_aggregate_plan: LogicalPlan,
    pub(crate) partition_key_indices: Vec<usize>,
    pub(crate) retract_injected_plan: LogicalPlan,
    pub(crate) namespace_qualifier: Option<TableReference>,
    pub(crate) state_retention_ttl: Duration,
}

impl ContinuousAggregateNode {
    pub fn try_new(
        base_aggregate_plan: LogicalPlan,
        partition_key_indices: Vec<usize>,
        namespace_qualifier: Option<TableReference>,
        state_retention_ttl: Duration,
    ) -> Result<Self> {
        let retract_injected_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(IsRetractExtension::new(
                base_aggregate_plan.clone(),
                namespace_qualifier.clone(),
            )),
        });

        Ok(Self {
            base_aggregate_plan,
            partition_key_indices,
            retract_injected_plan,
            namespace_qualifier,
            state_retention_ttl,
        })
    }

    fn construct_state_metadata_expr(&self, upstream_schema: &FsSchemaRef) -> Expr {
        let routing_keys: Vec<Expr> = self
            .partition_key_indices
            .iter()
            .map(|&idx| col(upstream_schema.schema.field(idx).name()))
            .collect();

        let state_id_hash = if routing_keys.is_empty() {
            Expr::Literal(
                ScalarValue::FixedSizeBinary(
                    STATIC_HASH_SIZE_BYTES,
                    Some(vec![0; STATIC_HASH_SIZE_BYTES as usize]),
                ),
                None,
            )
        } else {
            Expr::ScalarFunction(ScalarFunction {
                func: multi_hash(),
                args: routing_keys,
            })
        };

        named_struct(vec![
            lit(updating_state_field::IS_RETRACT),
            lit(false),
            lit(updating_state_field::ID),
            state_id_hash,
        ])
    }

    fn compile_operator_config(
        &self,
        planner: &Planner,
        upstream_schema: &FsSchemaRef,
    ) -> Result<UpdatingAggregateOperator> {
        let upstream_df_schema = upstream_schema.schema.clone().to_dfschema()?;

        let physical_agg_plan = planner.sync_plan(&self.base_aggregate_plan)?;
        let compiled_agg_payload = PhysicalPlanNode::try_from_physical_plan(
            physical_agg_plan,
            &FsPhysicalExtensionCodec::default(),
        )?
        .encode_to_vec();

        let meta_expr = self.construct_state_metadata_expr(upstream_schema);
        let compiled_meta_expr =
            planner.serialize_as_physical_expr(&meta_expr, &upstream_df_schema)?;

        Ok(UpdatingAggregateOperator {
            name: proto_operator_name::UPDATING_AGGREGATE.to_string(),
            input_schema: Some((**upstream_schema).clone().into()),
            final_schema: Some(self.yielded_schema().into()),
            aggregate_exec: compiled_agg_payload,
            metadata_expr: compiled_meta_expr,
            flush_interval_micros: DEFAULT_FLUSH_INTERVAL_MICROS,
            ttl_micros: self.state_retention_ttl.as_micros() as u64,
        })
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for ContinuousAggregateNode {
    fn name(&self) -> &str {
        CONTINUOUS_AGGREGATE_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.base_aggregate_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.retract_injected_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ContinuousAggregateNode(TTL={:?})",
            self.state_retention_ttl
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "ContinuousAggregateNode requires exactly 1 upstream input, got {}",
                inputs.len()
            );
        }

        Self::try_new(
            inputs.remove(0),
            self.partition_key_indices.clone(),
            self.namespace_qualifier.clone(),
            self.state_retention_ttl,
        )
    }
}

// -----------------------------------------------------------------------------
// Core Execution Blueprint Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for ContinuousAggregateNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_index: usize,
        mut upstream_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if upstream_schemas.len() != 1 {
            return plan_err!(
                "Topology Violation: ContinuousAggregateNode requires exactly 1 upstream input, received {}",
                upstream_schemas.len()
            );
        }

        let upstream_schema = upstream_schemas.remove(0);

        let operator_config = self.compile_operator_config(planner, &upstream_schema)?;

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("updating_aggregate_{node_index}"),
            OperatorName::UpdatingAggregate,
            operator_config.encode_to_vec(),
            proto_operator_name::UPDATING_AGGREGATE.to_string(),
            1,
        );

        let shuffle_edge =
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*upstream_schema).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![shuffle_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into()))
            .expect("Fatal: Failed to generate unkeyed output schema for continuous aggregate")
    }
}
