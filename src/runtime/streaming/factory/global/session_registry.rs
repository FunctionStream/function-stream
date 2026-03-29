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

//! 运行时 UDF / UDAF / UDWF 查询表（基于 DataFusion [`SessionContext`]）。

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

/// 为物理计划反序列化等路径提供 [`FunctionRegistry`] 实现。
///
/// 由 [`crate::runtime::streaming::factory::OperatorFactory`] 持有 `Arc<Registry>`，
/// 与各 [`crate::runtime::streaming::factory::OperatorConstructor`] 共享；须显式 [`Self::new`] 构造，无默认实例。
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
