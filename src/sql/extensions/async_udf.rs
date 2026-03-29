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
use std::time::Duration;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_common::{internal_err, plan_err};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;
use protocol::grpc::api::{AsyncUdfOperator, AsyncUdfOrdering};

use crate::multifield_partial_ord;
use crate::sql::common::constants::extension_node;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::streaming_operator_blueprint::{CompiledTopologyNode, StreamingOperatorBlueprint};
use crate::sql::logical_node::logical::{
    DylibUdfConfig, LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName,
};
use crate::sql::common::constants::sql_field;
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::types::{DFField, fields_with_qualifiers, schema_from_df_fields};

pub(crate) const NODE_TYPE_NAME: &str = extension_node::ASYNC_FUNCTION_EXECUTION;

/// Represents a logical node that executes an external asynchronous function (UDF)
/// and projects the final results into the streaming pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AsyncFunctionExecutionNode {
    pub(crate) upstream_plan: Arc<LogicalPlan>,
    pub(crate) operator_name: String,
    pub(crate) function_config: DylibUdfConfig,
    pub(crate) invocation_args: Vec<Expr>,
    pub(crate) result_projections: Vec<Expr>,
    pub(crate) preserve_ordering: bool,
    pub(crate) concurrency_limit: usize,
    pub(crate) execution_timeout: Duration,
    pub(crate) resolved_schema: DFSchemaRef,
}

multifield_partial_ord!(
    AsyncFunctionExecutionNode,
    upstream_plan,
    operator_name,
    function_config,
    invocation_args,
    result_projections,
    preserve_ordering,
    concurrency_limit,
    execution_timeout
);

impl AsyncFunctionExecutionNode {
    /// Compiles logical expressions into serialized physical protobuf bytes.
    fn compile_physical_expressions(
        &self,
        planner: &Planner,
        expressions: &[Expr],
        schema_context: &DFSchemaRef,
    ) -> Result<Vec<Vec<u8>>> {
        expressions
            .iter()
            .map(|logical_expr| {
                let physical_expr = planner.create_physical_expr(logical_expr, schema_context)?;
                let serialized =
                    serialize_physical_expr(&physical_expr, &DefaultPhysicalExtensionCodec {})?;
                Ok(serialized.encode_to_vec())
            })
            .collect()
    }

    /// Computes the intermediate schema which bridges the upstream output
    /// and the raw asynchronous result injected by the UDF execution.
    fn compute_intermediate_schema(&self) -> Result<DFSchemaRef> {
        let mut fields = fields_with_qualifiers(self.upstream_plan.schema());

        let raw_result_field = DFField::new(
            None,
            sql_field::ASYNC_RESULT,
            self.function_config.return_type.clone(),
            true,
        );
        fields.push(raw_result_field);

        Ok(Arc::new(schema_from_df_fields(&fields)?))
    }

    fn to_protobuf_config(
        &self,
        compiled_args: Vec<Vec<u8>>,
        compiled_projections: Vec<Vec<u8>>,
    ) -> AsyncUdfOperator {
        let ordering_strategy = if self.preserve_ordering {
            AsyncUdfOrdering::Ordered
        } else {
            AsyncUdfOrdering::Unordered
        };

        AsyncUdfOperator {
            name: self.operator_name.clone(),
            udf: Some(self.function_config.clone().into()),
            arg_exprs: compiled_args,
            final_exprs: compiled_projections,
            ordering: ordering_strategy as i32,
            max_concurrency: self.concurrency_limit as u32,
            timeout_micros: self.execution_timeout.as_micros() as u64,
        }
    }
}

impl StreamingOperatorBlueprint for AsyncFunctionExecutionNode {
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
            return plan_err!("AsyncFunctionExecutionNode requires exactly one input schema");
        }

        let compiled_args = self.compile_physical_expressions(
            planner,
            &self.invocation_args,
            self.upstream_plan.schema(),
        )?;

        let intermediate_schema = self.compute_intermediate_schema()?;
        let compiled_projections = self.compile_physical_expressions(
            planner,
            &self.result_projections,
            &intermediate_schema,
        )?;

        let operator_config = self.to_protobuf_config(compiled_args, compiled_projections);

        let logical_node = LogicalNode::single(
            node_index as u32,
            format!("async_udf_{node_index}"),
            OperatorName::AsyncUdf,
            operator_config.encode_to_vec(),
            format!("AsyncUdf<{}>", self.operator_name),
            1,
        );

        let upstream_schema = input_schemas.remove(0);
        let data_edge =
            LogicalEdge::project_all(LogicalEdgeType::Forward, (*upstream_schema).clone());

        Ok(CompiledTopologyNode {
            execution_unit: logical_node,
            routing_edges: vec![data_edge],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        let arrow_fields: Vec<_> = self
            .resolved_schema
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();

        FsSchema::from_fields(arrow_fields)
    }
}

impl UserDefinedLogicalNodeCore for AsyncFunctionExecutionNode {
    fn name(&self) -> &str {
        NODE_TYPE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.resolved_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.invocation_args
            .iter()
            .chain(self.result_projections.iter())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "AsyncFunctionExecution<{}>: Concurrency={}, Ordered={}",
            self.operator_name,
            self.concurrency_limit,
            self.preserve_ordering
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!(
                "AsyncFunctionExecutionNode expects exactly 1 input, but received {}",
                inputs.len()
            );
        }

        if UserDefinedLogicalNode::expressions(self) != exprs {
            return internal_err!(
                "Attempted to mutate async UDF expressions during logical planning, which is not supported."
            );
        }

        Ok(Self {
            upstream_plan: Arc::new(inputs.remove(0)),
            operator_name: self.operator_name.clone(),
            function_config: self.function_config.clone(),
            invocation_args: self.invocation_args.clone(),
            result_projections: self.result_projections.clone(),
            preserve_ordering: self.preserve_ordering,
            concurrency_limit: self.concurrency_limit,
            execution_timeout: self.execution_timeout,
            resolved_schema: self.resolved_schema.clone(),
        })
    }
}
