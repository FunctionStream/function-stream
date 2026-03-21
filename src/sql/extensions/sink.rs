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

use datafusion::common::{DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use prost::Message;

use crate::multifield_partial_ord;
use crate::sql::common::{FsSchema, FsSchemaRef, UPDATING_META_FIELD};
use crate::sql::extensions::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::schema::Table;

use super::debezium::PackDebeziumEnvelopeNode;
use super::remote_table::RemoteTableBoundaryNode;

// -----------------------------------------------------------------------------
// Constants & Identifiers
// -----------------------------------------------------------------------------

pub(crate) const STREAM_EGRESS_NODE_NAME: &str = "StreamEgressNode";

// -----------------------------------------------------------------------------
// Logical Node Definition
// -----------------------------------------------------------------------------

/// Terminal node routing processed data into an external sink (e.g. Kafka, PostgreSQL).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamEgressNode {
    pub(crate) target_identifier: TableReference,
    pub(crate) destination_table: Table,
    pub(crate) egress_schema: DFSchemaRef,
    upstream_plans: Arc<Vec<LogicalPlan>>,
}

multifield_partial_ord!(StreamEgressNode, target_identifier, upstream_plans);

impl StreamEgressNode {
    pub fn try_new(
        target_identifier: TableReference,
        destination_table: Table,
        initial_schema: DFSchemaRef,
        upstream_plan: LogicalPlan,
    ) -> Result<Self> {
        let (mut processed_plan, mut resolved_schema) = Self::apply_cdc_transformations(
            upstream_plan,
            initial_schema,
            &destination_table,
        )?;

        Self::enforce_computational_boundary(&mut resolved_schema, &mut processed_plan);

        Ok(Self {
            target_identifier,
            destination_table,
            egress_schema: resolved_schema,
            upstream_plans: Arc::new(vec![processed_plan]),
        })
    }

    fn apply_cdc_transformations(
        plan: LogicalPlan,
        schema: DFSchemaRef,
        destination: &Table,
    ) -> Result<(LogicalPlan, DFSchemaRef)> {
        let is_upstream_updating = plan
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD);

        match destination {
            Table::ConnectorTable(connector) => {
                let is_sink_updating = connector.is_updating();

                match (is_upstream_updating, is_sink_updating) {
                    (_, true) => {
                        let debezium_encoder = PackDebeziumEnvelopeNode::try_new(plan)?;
                        let wrapped_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(debezium_encoder),
                        });
                        let new_schema = wrapped_plan.schema().clone();

                        Ok((wrapped_plan, new_schema))
                    }
                    (true, false) => {
                        plan_err!(
                            "Topology Mismatch: The upstream is producing an updating stream (CDC), \
                             but the target sink '{}' is not configured to accept updates. \
                             Hint: set `format = 'debezium_json'` in the WITH clause.",
                            connector.name()
                        )
                    }
                    (false, false) => Ok((plan, schema)),
                }
            }
            Table::LookupTable(..) => {
                plan_err!("Topology Violation: A Lookup Table cannot be used as a streaming data sink.")
            }
            Table::TableFromQuery { .. } => Ok((plan, schema)),
        }
    }

    fn enforce_computational_boundary(schema: &mut DFSchemaRef, plan: &mut LogicalPlan) {
        let requires_boundary = if let LogicalPlan::Extension(extension) = plan {
            let stream_ext: &dyn StreamingOperatorBlueprint = (&extension.node)
                .try_into()
                .expect("Fatal: Egress node encountered an extension that does not implement StreamingOperatorBlueprint");

            stream_ext.is_passthrough_boundary()
        } else {
            true
        };

        if requires_boundary {
            let boundary_node = RemoteTableBoundaryNode {
                upstream_plan: plan.clone(),
                table_identifier: TableReference::bare("sink projection"),
                resolved_schema: schema.clone(),
                requires_materialization: false,
            };

            *plan = LogicalPlan::Extension(Extension {
                node: Arc::new(boundary_node),
            });
        }
    }
}

// -----------------------------------------------------------------------------
// Stream Extension Trait Implementation
// -----------------------------------------------------------------------------

impl StreamingOperatorBlueprint for StreamEgressNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        Some(NamedNode::Sink(self.target_identifier.clone()))
    }

    fn compile_to_graph_node(
        &self,
        _planner: &Planner,
        node_index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        let connector_operator = self
            .destination_table
            .connector_op()
            .map_err(|e| e.context("Failed to generate connector operation payload"))?;

        let operator_description = connector_operator.description.clone();
        let operator_payload = connector_operator.encode_to_vec();

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("sink_{}_{node_index}", self.target_identifier),
            OperatorName::ConnectorSink,
            operator_payload,
            operator_description,
            1,
        );

        let routing_edges: Vec<LogicalEdge> = input_schemas
            .into_iter()
            .map(|input_schema| {
                LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema).clone())
            })
            .collect();

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: routing_edges,
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_fields(vec![])
    }
}

// -----------------------------------------------------------------------------
// DataFusion Logical Node Hooks
// -----------------------------------------------------------------------------

impl UserDefinedLogicalNodeCore for StreamEgressNode {
    fn name(&self) -> &str {
        STREAM_EGRESS_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.upstream_plans.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.egress_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamEgressNode({:?}): Schema={}",
            self.target_identifier, self.egress_schema
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            target_identifier: self.target_identifier.clone(),
            destination_table: self.destination_table.clone(),
            egress_schema: self.egress_schema.clone(),
            upstream_plans: Arc::new(inputs),
        })
    }
}
