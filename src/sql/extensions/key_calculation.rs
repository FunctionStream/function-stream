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

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchemaRef, Result, internal_err, plan_err};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::DFSchema;
use datafusion_expr::col;
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalPlanNode;
use itertools::Itertools;
use prost::Message;

use protocol::grpc::api::{KeyPlanOperator, ProjectionOperator};

use crate::multifield_partial_ord;
use crate::sql::common::constants::{extension_node, sql_field};
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::physical::FsPhysicalExtensionCodec;
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::types::{fields_with_qualifiers, schema_from_df_fields_with_metadata};

pub(crate) const EXTENSION_NODE_IDENTIFIER: &str = extension_node::KEY_EXTRACTION;

/// Routing strategy for shuffling data across the stream topology.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum KeyExtractionStrategy {
    ColumnIndices(Vec<usize>),
    CalculatedExpressions(Vec<Expr>),
}

/// Logical node that computes or extracts routing keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeyExtractionNode {
    pub(crate) operator_label: Option<String>,
    pub(crate) upstream_plan: LogicalPlan,
    pub(crate) extraction_strategy: KeyExtractionStrategy,
    pub(crate) resolved_schema: DFSchemaRef,
}

multifield_partial_ord!(
    KeyExtractionNode,
    operator_label,
    upstream_plan,
    extraction_strategy
);

impl KeyExtractionNode {
    /// Extracts keys and hides them from the downstream projection.
    pub fn try_new_with_projection(
        upstream_plan: LogicalPlan,
        target_indices: Vec<usize>,
        label: String,
    ) -> Result<Self> {
        let projected_fields: Vec<_> = fields_with_qualifiers(upstream_plan.schema())
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| !target_indices.contains(idx))
            .map(|(_, field)| field)
            .collect();

        let metadata = upstream_plan.schema().metadata().clone();
        let resolved_schema = schema_from_df_fields_with_metadata(&projected_fields, metadata)?;

        Ok(Self {
            operator_label: Some(label),
            upstream_plan,
            extraction_strategy: KeyExtractionStrategy::ColumnIndices(target_indices),
            resolved_schema: Arc::new(resolved_schema),
        })
    }

    /// Creates a node using an explicit strategy without changing the visible schema.
    pub fn new(upstream_plan: LogicalPlan, strategy: KeyExtractionStrategy) -> Self {
        let resolved_schema = upstream_plan.schema().clone();
        Self {
            operator_label: None,
            upstream_plan,
            extraction_strategy: strategy,
            resolved_schema,
        }
    }

    fn compile_index_router(
        &self,
        physical_plan_proto: PhysicalPlanNode,
        indices: &[usize],
    ) -> (Vec<u8>, OperatorName) {
        let operator_config = KeyPlanOperator {
            name: sql_field::DEFAULT_KEY_LABEL.into(),
            physical_plan: physical_plan_proto.encode_to_vec(),
            key_fields: indices.iter().map(|&idx| idx as u64).collect(),
        };

        (operator_config.encode_to_vec(), OperatorName::KeyBy)
    }

    fn compile_expression_router(
        &self,
        planner: &Planner,
        expressions: &[Expr],
        input_schema_ref: &FsSchemaRef,
        input_df_schema: &DFSchemaRef,
    ) -> Result<(Vec<u8>, OperatorName)> {
        let mut target_exprs = expressions.to_vec();

        for field in input_schema_ref.schema.fields.iter() {
            target_exprs.push(col(field.name()));
        }

        let output_fs_schema = self.generate_fs_schema()?;

        for (compiled_expr, expected_field) in target_exprs
            .iter()
            .zip(output_fs_schema.schema.fields())
        {
            let (expr_type, expr_nullable) = compiled_expr.data_type_and_nullable(input_df_schema)?;
            if expr_type != *expected_field.data_type() || expr_nullable != expected_field.is_nullable()
            {
                return plan_err!(
                    "Type mismatch in key calculation: Expected {} (nullable: {}), got {} (nullable: {})",
                    expected_field.data_type(),
                    expected_field.is_nullable(),
                    expr_type,
                    expr_nullable
                );
            }
        }

        let mut physical_expr_payloads = Vec::with_capacity(target_exprs.len());
        for logical_expr in target_exprs {
            let physical_expr = planner
                .create_physical_expr(&logical_expr, input_df_schema)
                .map_err(|e| e.context("Failed to physicalize PARTITION BY expression"))?;

            let serialized_expr =
                serialize_physical_expr(&physical_expr, &DefaultPhysicalExtensionCodec {})?;
            physical_expr_payloads.push(serialized_expr.encode_to_vec());
        }

        let operator_config = ProjectionOperator {
            name: self
                .operator_label
                .as_deref()
                .unwrap_or(sql_field::DEFAULT_KEY_LABEL)
                .to_string(),
            input_schema: Some(input_schema_ref.as_ref().clone().into()),
            output_schema: Some(output_fs_schema.into()),
            exprs: physical_expr_payloads,
        };

        Ok((operator_config.encode_to_vec(), OperatorName::Projection))
    }

    fn generate_fs_schema(&self) -> Result<FsSchema> {
        let base_arrow_schema = self.upstream_plan.schema().as_ref();

        match &self.extraction_strategy {
            KeyExtractionStrategy::ColumnIndices(indices) => {
                FsSchema::from_schema_keys(Arc::new(base_arrow_schema.into()), indices.clone())
            }
            KeyExtractionStrategy::CalculatedExpressions(expressions) => {
                let mut composite_fields =
                    Vec::with_capacity(expressions.len() + base_arrow_schema.fields().len());

                for (idx, expr) in expressions.iter().enumerate() {
                    let (data_type, nullable) = expr.data_type_and_nullable(base_arrow_schema)?;
                    composite_fields.push(Field::new(format!("__key_{idx}"), data_type, nullable).into());
                }

                for field in base_arrow_schema.fields().iter() {
                    composite_fields.push(field.clone());
                }

                let final_schema = Arc::new(Schema::new(composite_fields));
                let key_mapping = (1..=expressions.len()).collect_vec();
                FsSchema::from_schema_keys(final_schema, key_mapping)
            }
        }
    }
}

impl StreamingOperatorBlueprint for KeyExtractionNode {
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
            return plan_err!("KeyExtractionNode requires exactly one upstream input schema");
        }

        let input_schema_ref = input_schemas.remove(0);
        let input_df_schema = Arc::new(DFSchema::try_from(input_schema_ref.schema.as_ref().clone())?);

        let physical_plan = planner.sync_plan(&self.upstream_plan)?;
        let physical_plan_proto = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &FsPhysicalExtensionCodec::default(),
        )?;

        let (protobuf_payload, engine_operator_name) = match &self.extraction_strategy {
            KeyExtractionStrategy::ColumnIndices(indices) => {
                self.compile_index_router(physical_plan_proto, indices)
            }
            KeyExtractionStrategy::CalculatedExpressions(exprs) => {
                self.compile_expression_router(planner, exprs, &input_schema_ref, &input_df_schema)?
            }
        };

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("key_{node_index}"),
            engine_operator_name,
            protobuf_payload,
            format!("ArrowKey<{}>", self.operator_label.as_deref().unwrap_or("_")),
            1,
        );

        let data_edge =
            LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema_ref).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![data_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        self.generate_fs_schema()
            .expect("Fatal: Failed to generate output schema for KeyExtractionNode")
    }
}

impl UserDefinedLogicalNodeCore for KeyExtractionNode {
    fn name(&self) -> &str {
        EXTENSION_NODE_IDENTIFIER
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
            "KeyExtractionNode: Strategy={:?} | Schema={}",
            self.extraction_strategy,
            self.resolved_schema
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("KeyExtractionNode requires exactly 1 input logical plan");
        }

        let strategy = match &self.extraction_strategy {
            KeyExtractionStrategy::ColumnIndices(indices) => {
                KeyExtractionStrategy::ColumnIndices(indices.clone())
            }
            KeyExtractionStrategy::CalculatedExpressions(_) => {
                KeyExtractionStrategy::CalculatedExpressions(exprs)
            }
        };

        Ok(Self {
            operator_label: self.operator_label.clone(),
            upstream_plan: inputs.remove(0),
            extraction_strategy: strategy,
            resolved_schema: self.resolved_schema.clone(),
        })
    }
}
