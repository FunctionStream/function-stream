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

use anyhow::{anyhow, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::sync::Arc;

use protocol::grpc::api::ProjectionOperator as ProjectionOperatorProto;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{ConstructedOperator, Operator};
use crate::runtime::streaming::factory::global::Registry;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, FsSchema, FsSchemaRef, Watermark};
use crate::sql::logical_node::logical::OperatorName;

pub struct ProjectionOperator {
    name: String,
    output_schema: FsSchemaRef,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl ProjectionOperator {
    pub fn new(
        name: String,
        output_schema: FsSchemaRef,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            name,
            output_schema,
            exprs,
        }
    }

    pub fn from_proto(
        config: ProjectionOperatorProto,
        registry: Arc<Registry>,
    ) -> Result<Self> {
        let input_schema: FsSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("missing projection input_schema"))?
            .try_into()
            .map_err(|e| anyhow!("projection input_schema: {e}"))?;

        let output_schema: FsSchema = config
            .output_schema
            .ok_or_else(|| anyhow!("missing projection output_schema"))?
            .try_into()
            .map_err(|e| anyhow!("projection output_schema: {e}"))?;

        let exprs = config
            .exprs
            .iter()
            .map(|raw| {
                let expr_node = PhysicalExprNode::decode(&mut raw.as_slice())
                    .map_err(|e| anyhow!("decode projection expr: {e}"))?;
                parse_physical_expr(
                    &expr_node,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )
                    .map_err(|e| anyhow!("parse projection expr: {e}"))
            })
            .collect::<Result<Vec<_>>>()?;

        let name = if config.name.is_empty() {
            OperatorName::Projection.as_registry_key().to_string()
        } else {
            config.name
        };

        Ok(Self::new(name, Arc::new(output_schema), exprs))

    }
}

#[async_trait]
impl Operator for ProjectionOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let projected_columns = self
            .exprs
            .iter()
            .map(|expr| {
                expr.evaluate(&batch)
                    .and_then(|val| val.into_array(batch.num_rows()))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let out_batch =
            RecordBatch::try_new(self.output_schema.schema.clone(), projected_columns)?;

        Ok(vec![StreamOutput::Forward(out_batch)])
    }

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        Ok(vec![StreamOutput::Watermark(watermark)])
    }

    async fn snapshot_state(
        &mut self,
        _barrier: CheckpointBarrier,
        _ctx: &mut TaskContext,
    ) -> Result<()> {
        Ok(())
    }
}
