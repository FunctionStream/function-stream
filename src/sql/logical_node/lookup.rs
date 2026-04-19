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

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{Column, DFSchemaRef, JoinType, Result, internal_err, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::TableReference;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;

use protocol::function_stream_graph;
use protocol::function_stream_graph::{
    ConnectorOp, GenericConnectorConfig, LookupJoinCondition, LookupJoinOperator,
};

use crate::multifield_partial_ord;
use crate::sql::common::constants::extension_node;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_node::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::schema::SourceTable;
use crate::sql::schema::utils::add_timestamp_field_arrow;

pub const DICTIONARY_SOURCE_NODE_NAME: &str = extension_node::REFERENCE_TABLE_SOURCE;
pub const STREAM_DICTIONARY_JOIN_NODE_NAME: &str = extension_node::STREAM_REFERENCE_JOIN;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReferenceTableSourceNode {
    pub(crate) source_definition: SourceTable,
    pub(crate) resolved_schema: DFSchemaRef,
}

multifield_partial_ord!(ReferenceTableSourceNode, source_definition);

impl UserDefinedLogicalNodeCore for ReferenceTableSourceNode {
    fn name(&self) -> &str {
        DICTIONARY_SOURCE_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ReferenceTableSource: Schema={}", self.resolved_schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !inputs.is_empty() {
            return internal_err!(
                "ReferenceTableSource is a leaf node and cannot accept upstream inputs"
            );
        }

        Ok(Self {
            source_definition: self.source_definition.clone(),
            resolved_schema: self.resolved_schema.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamReferenceJoinNode {
    pub(crate) upstream_stream_plan: LogicalPlan,
    pub(crate) output_schema: DFSchemaRef,
    pub(crate) external_dictionary: SourceTable,
    pub(crate) equijoin_conditions: Vec<(Expr, Column)>,
    pub(crate) post_join_filter: Option<Expr>,
    pub(crate) namespace_alias: Option<TableReference>,
    pub(crate) join_semantics: JoinType,
}

multifield_partial_ord!(
    StreamReferenceJoinNode,
    upstream_stream_plan,
    external_dictionary,
    equijoin_conditions,
    post_join_filter,
    namespace_alias
);

impl StreamReferenceJoinNode {
    fn compile_join_conditions(&self, planner: &Planner) -> Result<Vec<LookupJoinCondition>> {
        self.equijoin_conditions
            .iter()
            .map(|(logical_left_expr, right_column)| {
                let physical_expr =
                    planner.create_physical_expr(logical_left_expr, &self.output_schema)?;
                let serialized_expr =
                    serialize_physical_expr(&physical_expr, &DefaultPhysicalExtensionCodec {})?;

                Ok(LookupJoinCondition {
                    left_expr: serialized_expr.encode_to_vec(),
                    right_key: right_column.name.clone(),
                })
            })
            .collect()
    }

    fn map_api_join_type(&self) -> Result<i32> {
        match self.join_semantics {
            JoinType::Inner => Ok(function_stream_graph::JoinType::Inner as i32),
            JoinType::Left => Ok(function_stream_graph::JoinType::Left as i32),
            unsupported => plan_err!(
                "Unsupported join type '{unsupported}' for dictionary lookups. Only INNER and LEFT joins are permitted."
            ),
        }
    }

    fn build_engine_operator(
        &self,
        planner: &Planner,
        _upstream_schema: &FsSchemaRef,
    ) -> Result<LookupJoinOperator> {
        let internal_input_schema =
            FsSchema::from_schema_unkeyed(Arc::new(self.output_schema.as_ref().into()))?;
        let dictionary_physical_schema = self.external_dictionary.produce_physical_schema();
        let lookup_fs_schema =
            FsSchema::from_schema_unkeyed(add_timestamp_field_arrow(dictionary_physical_schema))?;

        let properties: HashMap<String, String> = self
            .external_dictionary
            .catalog_with_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(LookupJoinOperator {
            input_schema: Some(internal_input_schema.into()),
            lookup_schema: Some(lookup_fs_schema.clone().into()),
            connector: Some(ConnectorOp {
                connector: self.external_dictionary.adapter_type.clone(),
                fs_schema: Some(lookup_fs_schema.into()),
                name: self.external_dictionary.table_identifier.clone(),
                description: self.external_dictionary.description.clone(),
                config: Some(
                    protocol::function_stream_graph::connector_op::Config::Generic(
                        GenericConnectorConfig { properties },
                    ),
                ),
            }),
            key_exprs: self.compile_join_conditions(planner)?,
            join_type: self.map_api_join_type()?,
            ttl_micros: self
                .external_dictionary
                .lookup_cache_ttl
                .map(|t| t.as_micros() as u64),
            max_capacity_bytes: self.external_dictionary.lookup_cache_max_bytes,
        })
    }
}

impl StreamingOperatorBlueprint for StreamReferenceJoinNode {
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
            return plan_err!("StreamReferenceJoinNode requires exactly one upstream stream input");
        }
        let upstream_schema = input_schemas.remove(0);

        let operator_config = self.build_engine_operator(planner, &upstream_schema)?;

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("lookup_join_{node_index}"),
            OperatorName::LookupJoin,
            operator_config.encode_to_vec(),
            format!(
                "DictionaryJoin<{}>",
                self.external_dictionary.table_identifier
            ),
            planner.default_parallelism(),
        );

        let incoming_edge =
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*upstream_schema).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![incoming_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        FsSchema::from_schema_unkeyed(self.output_schema.inner().clone())
            .expect("Failed to convert lookup join output schema to FsSchema")
    }
}

impl UserDefinedLogicalNodeCore for StreamReferenceJoinNode {
    fn name(&self) -> &str {
        STREAM_DICTIONARY_JOIN_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_stream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs: Vec<_> = self
            .equijoin_conditions
            .iter()
            .map(|(l, _)| l.clone())
            .collect();
        if let Some(filter) = &self.post_join_filter {
            exprs.push(filter.clone());
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamReferenceJoin: join_type={:?} | {}",
            self.join_semantics, self.output_schema
        )
    }

    fn with_exprs_and_inputs(&self, _: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "StreamReferenceJoinNode expects exactly 1 upstream plan, got {}",
                inputs.len()
            );
        }
        Ok(Self {
            upstream_stream_plan: inputs[0].clone(),
            output_schema: self.output_schema.clone(),
            external_dictionary: self.external_dictionary.clone(),
            equijoin_conditions: self.equijoin_conditions.clone(),
            post_join_filter: self.post_join_filter.clone(),
            namespace_alias: self.namespace_alias.clone(),
            join_semantics: self.join_semantics,
        })
    }
}
