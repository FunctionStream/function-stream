//! 通用无状态执行算子：驱动 DataFusion 物理计划（Filter, Case When, Scalar UDF 等），
//! 不改变分区状态，适用于 Map / Filter 阶段。

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::operators::StatelessPhysicalExecutor;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};

pub struct ValueExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
}

impl ValueExecutionOperator {
    pub fn new(name: String, executor: StatelessPhysicalExecutor) -> Self {
        Self { name, executor }
    }
}

#[async_trait]
impl MessageOperator for ValueExecutionOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let mut outputs = Vec::new();

        let mut stream = self.executor.process_batch(batch).await?;

        while let Some(batch_result) = stream.next().await {
            let out_batch = batch_result?;
            if out_batch.num_rows() > 0 {
                outputs.push(StreamOutput::Forward(out_batch));
            }
        }
        Ok(outputs)
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
