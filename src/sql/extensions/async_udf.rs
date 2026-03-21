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
use datafusion_common::internal_err;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;
use protocol::grpc::api::{AsyncUdfOperator, AsyncUdfOrdering};

use crate::multifield_partial_ord;
use crate::sql::extensions::constants::ASYNC_RESULT_FIELD;
use crate::sql::extensions::stream_extension::{NodeWithIncomingEdges, StreamExtension};
use crate::sql::logical_node::logical::{
    DylibUdfConfig, LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName,
};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::types::{DFField, fields_with_qualifiers, schema_from_df_fields};
use crate::sql::common::{FsSchema, FsSchemaRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AsyncUDFExtension {
    pub(crate) input: Arc<LogicalPlan>,
    pub(crate) name: String,
    pub(crate) udf: DylibUdfConfig,
    pub(crate) arg_exprs: Vec<Expr>,
    pub(crate) final_exprs: Vec<Expr>,
    pub(crate) ordered: bool,
    pub(crate) max_concurrency: usize,
    pub(crate) timeout: Duration,
    pub(crate) final_schema: DFSchemaRef,
}

multifield_partial_ord!(
    AsyncUDFExtension,
    input,
    name,
    udf,
    arg_exprs,
    final_exprs,
    ordered,
    max_concurrency,
    timeout
);

impl StreamExtension for AsyncUDFExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        let arg_exprs = self
            .arg_exprs
            .iter()
            .map(|e| {
                let p = planner.create_physical_expr(e, self.input.schema())?;
                Ok(serialize_physical_expr(&p, &DefaultPhysicalExtensionCodec {})?.encode_to_vec())
            })
            .collect::<Result<Vec<_>>>()?;

        let mut final_fields = fields_with_qualifiers(self.input.schema());
        final_fields.push(DFField::new(
            None,
            ASYNC_RESULT_FIELD,
            self.udf.return_type.clone(),
            true,
        ));
        let post_udf_schema = schema_from_df_fields(&final_fields)?;

        let final_exprs = self
            .final_exprs
            .iter()
            .map(|e| {
                let p = planner.create_physical_expr(e, &post_udf_schema)?;
                Ok(serialize_physical_expr(&p, &DefaultPhysicalExtensionCodec {})?.encode_to_vec())
            })
            .collect::<Result<Vec<_>>>()?;

        let config = AsyncUdfOperator {
            name: self.name.clone(),
            udf: Some(self.udf.clone().into()),
            arg_exprs,
            final_exprs,
            ordering: if self.ordered {
                AsyncUdfOrdering::Ordered as i32
            } else {
                AsyncUdfOrdering::Unordered as i32
            },
            max_concurrency: self.max_concurrency as u32,
            timeout_micros: self.timeout.as_micros() as u64,
        };

        let node = LogicalNode::single(
            index as u32,
            format!("async_udf_{index}"),
            OperatorName::AsyncUdf,
            config.encode_to_vec(),
            format!("async_udf<{}>", self.name),
            1,
        );

        let incoming_edge =
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schemas[0].as_ref().clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![incoming_edge],
        })
    }

    fn output_schema(&self) -> FsSchema {
        FsSchema::from_fields(
            self.final_schema
                .fields()
                .iter()
                .map(|f| (**f).clone())
                .collect(),
        )
    }
}

impl UserDefinedLogicalNodeCore for AsyncUDFExtension {
    fn name(&self) -> &str {
        "AsyncUDFNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.final_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.arg_exprs
            .iter()
            .chain(self.final_exprs.iter())
            .map(|e| e.to_owned())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "AsyncUdfExtension<{}>: {}", self.name, self.final_schema)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }
        if UserDefinedLogicalNode::expressions(self) != exprs {
            return internal_err!("Tried to recreate async UDF node with different expressions");
        }

        Ok(Self {
            input: Arc::new(inputs[0].clone()),
            name: self.name.clone(),
            udf: self.udf.clone(),
            arg_exprs: self.arg_exprs.clone(),
            final_exprs: self.final_exprs.clone(),
            ordered: self.ordered,
            max_concurrency: self.max_concurrency,
            timeout: self.timeout,
            final_schema: self.final_schema.clone(),
        })
    }
}
