//! 物理网络路由算子：利用 DataFusion 物理表达式提取 Key，基于 Hash 排序执行零拷贝切片路由。
//!
//! 提供两种算子：
//! - [`KeyByOperator`]：纯 Key 提取 + Hash 路由，适用于简单的 GROUP BY / PARTITION BY。
//! - [`KeyExecutionOperator`]：先执行完整物理计划，再按指定列 Hash 路由，适用于需要先做
//!   计算（如聚合结果映射）再分区的场景。

use anyhow::{anyhow, Result};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{sort_to_indices, take};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_common::hash_utils::create_hashes;
use futures::StreamExt;
use std::sync::Arc;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::operators::StatelessPhysicalExecutor;
use crate::runtime::streaming::StreamOutput;
use crate::sql::common::{CheckpointBarrier, Watermark};

use protocol::grpc::api::KeyPlanOperator;

pub struct KeyByOperator {
    name: String,
    key_extractors: Vec<Arc<dyn PhysicalExpr>>,
    random_state: ahash::RandomState,
}

impl KeyByOperator {
    pub fn new(name: String, key_extractors: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            name,
            key_extractors,
            random_state: ahash::RandomState::new(),
        }
    }
}

#[async_trait]
impl MessageOperator for KeyByOperator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> Result<()> {
        Ok(())
    }

    async fn process_data(
        &mut self,
        _input_idx: usize,
        batch: RecordBatch,
        _ctx: &mut TaskContext,
    ) -> Result<Vec<StreamOutput>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![]);
        }

        // 1. 执行物理表达式，提取所有 Key 列
        let mut key_columns = Vec::with_capacity(self.key_extractors.len());
        for expr in &self.key_extractors {
            let column_array = expr
                .evaluate(&batch)
                .map_err(|e| anyhow!("Failed to evaluate key expr: {}", e))?
                .into_array(num_rows)
                .map_err(|e| anyhow!("Failed to convert into array: {}", e))?;
            key_columns.push(column_array);
        }

        // 2. 向量化计算 Hash 数组
        let mut hash_buffer = vec![0u64; num_rows];
        create_hashes(&key_columns, &self.random_state, &mut hash_buffer)
            .map_err(|e| anyhow!("Failed to compute hashes: {}", e))?;

        let hash_array = UInt64Array::from(hash_buffer);

        // 3. 基于 Hash 值排序，获取重排 Indices
        let sorted_indices = sort_to_indices(&hash_array, None, None)
            .map_err(|e| anyhow!("Failed to sort hashes: {}", e))?;

        // 4. 对齐重排 Hash 数组和原始 Batch
        let sorted_hashes_ref = take(&hash_array, &sorted_indices, None)?;
        let sorted_hashes = sorted_hashes_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let sorted_columns: std::result::Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col, &sorted_indices, None))
            .collect();
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns?)?;

        // 5. 零拷贝微批切片 —— 按 Hash 值连续段切分并标记路由意图
        let mut outputs = Vec::new();
        let mut start_idx = 0;

        while start_idx < num_rows {
            let current_hash = sorted_hashes.value(start_idx);
            let mut end_idx = start_idx + 1;
            while end_idx < num_rows && sorted_hashes.value(end_idx) == current_hash {
                end_idx += 1;
            }

            let sub_batch = sorted_batch.slice(start_idx, end_idx - start_idx);
            outputs.push(StreamOutput::Keyed(current_hash, sub_batch));
            start_idx = end_idx;
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

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

pub struct KeyByConstructor;

impl KeyByConstructor {
    pub fn with_config(&self, config: KeyPlanOperator) -> Result<KeyByOperator> {
        let mut key_extractors: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(config.key_fields.len());

        for field_idx in &config.key_fields {
            let idx = *field_idx as usize;
            let expr = Arc::new(Column::new(&format!("col_{}", idx), idx))
                as Arc<dyn PhysicalExpr>;
            key_extractors.push(expr);
        }

        let name = if config.name.is_empty() {
            "KeyBy".to_string()
        } else {
            config.name.clone()
        };

        Ok(KeyByOperator::new(name, key_extractors))
    }
}

// ===========================================================================
// KeyExecutionOperator — 先执行物理计划，再按 Key 列 Hash 路由
// ===========================================================================

/// 键控路由执行算子：先驱动 DataFusion 物理计划完成计算（如聚合结果映射），
/// 再根据 `key_fields` 指定列计算 Hash 并以 [`StreamOutput::Keyed`] 输出，
/// 实现算子内部分区。
pub struct KeyExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
    key_fields: Vec<usize>,
    random_state: ahash::RandomState,
}

impl KeyExecutionOperator {
    pub fn new(
        name: String,
        executor: StatelessPhysicalExecutor,
        key_fields: Vec<usize>,
    ) -> Self {
        Self {
            name,
            executor,
            key_fields,
            random_state: ahash::RandomState::new(),
        }
    }
}

#[async_trait]
impl MessageOperator for KeyExecutionOperator {
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

        // 1. 执行物理转换
        let mut stream = self.executor.process_batch(batch).await?;

        while let Some(batch_result) = stream.next().await {
            let out_batch = batch_result?;
            let num_rows = out_batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            // 2. 提取 Key 列并计算 Hash
            let key_columns: Vec<ArrayRef> = self
                .key_fields
                .iter()
                .map(|&idx| out_batch.column(idx).clone())
                .collect();

            let mut hash_buffer = vec![0u64; num_rows];
            create_hashes(&key_columns, &self.random_state, &mut hash_buffer)
                .map_err(|e| anyhow!("hash compute: {e}"))?;
            let hash_array = UInt64Array::from(hash_buffer);

            // 3. 基于 Hash 排序，获取重排 Indices
            let sorted_indices = sort_to_indices(&hash_array, None, None)
                .map_err(|e| anyhow!("sort hashes: {e}"))?;

            let sorted_hashes_ref = take(&hash_array, &sorted_indices, None)?;
            let sorted_hashes = sorted_hashes_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let sorted_columns: std::result::Result<Vec<_>, _> = out_batch
                .columns()
                .iter()
                .map(|col| take(col, &sorted_indices, None))
                .collect();
            let sorted_batch =
                RecordBatch::try_new(out_batch.schema(), sorted_columns?)?;

            // 4. 零拷贝切片 —— 按 Hash 连续段分组，标记 Keyed 路由意图
            let mut start_idx = 0;
            while start_idx < num_rows {
                let current_hash = sorted_hashes.value(start_idx);
                let mut end_idx = start_idx + 1;
                while end_idx < num_rows
                    && sorted_hashes.value(end_idx) == current_hash
                {
                    end_idx += 1;
                }

                let sub_batch = sorted_batch.slice(start_idx, end_idx - start_idx);
                outputs.push(StreamOutput::Keyed(current_hash, sub_batch));
                start_idx = end_idx;
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

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}

