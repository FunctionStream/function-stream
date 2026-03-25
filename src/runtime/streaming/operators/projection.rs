//! 高性能投影算子：直接操作 Arrow Array 执行列映射与标量运算，
//! 避开 DataFusion 执行树开销，适用于 SELECT 字段筛选和简单标量计算。

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, FsSchemaRef, Watermark};

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
}

#[async_trait]
impl MessageOperator for ProjectionOperator {
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
