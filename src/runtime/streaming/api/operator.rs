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

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::SourceOperator;
use crate::runtime::streaming::protocol::stream_out::StreamOutput;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::logical_expr::planner::ExprPlanner;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use crate::sql::common::{CheckpointBarrier, Watermark};

// ---------------------------------------------------------------------------
// Registry — 算子 / UDF 注册表（取代 tracing_subscriber::Registry）
// ---------------------------------------------------------------------------

/// 运行时函数与状态注册表。
///
/// 包装 DataFusion [`SessionContext`]，为物理计划反序列化提供 UDF / UDAF / UDWF 查询能力。
/// `Arc<Registry>` 在工厂中创建后，由各构造器共享。
pub struct Registry {
    ctx: SessionContext,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        self.ctx.udfs()
    }

    fn udf(&self, name: &str) -> DfResult<Arc<ScalarUDF>> {
        self.ctx.udf(name)
    }

    fn udaf(&self, name: &str) -> DfResult<Arc<AggregateUDF>> {
        self.ctx.udaf(name)
    }

    fn udwf(&self, name: &str) -> DfResult<Arc<WindowUDF>> {
        self.ctx.udwf(name)
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.ctx.expr_planners()
    }
}

// ---------------------------------------------------------------------------
// ConstructedOperator
// ---------------------------------------------------------------------------

/// 工厂反射产出的具体算子实例
pub enum ConstructedOperator {
    Source(Box<dyn SourceOperator>),
    Operator(Box<dyn MessageOperator>),
}

/// 多上游、被动驱动的消息算子。
#[async_trait]
pub trait MessageOperator: Send + 'static {
    fn name(&self) -> &str;

    async fn on_start(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// `input_idx`：多输入拓扑下第几条边（与 `SubtaskRunner` 的 inbox 下标一致；单输入恒为 0）。
    async fn process_data(
        &mut self,
        input_idx: usize,
        batch: RecordBatch,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>>;

    async fn process_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>>;

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()>;

    /// 全局 checkpoint 确认后由 `SubtaskRunner` 在 [`ControlCommand::Commit`] 上调用（如 Kafka EOS 二阶段提交）。
    async fn commit_checkpoint(
        &mut self,
        _epoch: u32,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 周期性时钟（如 Idle 检测）；`None` 表示不注册 tick。
    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    /// 与 [`Self::tick_interval`] 配套，由 `SubtaskRunner` 按固定间隔调用。
    async fn process_tick(
        &mut self,
        _tick_index: u64,
        _ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }

    async fn on_close(&mut self, _ctx: &mut TaskContext) -> anyhow::Result<Vec<StreamOutput>> {
        Ok(vec![])
    }
}
