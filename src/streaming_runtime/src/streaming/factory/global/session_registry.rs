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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

/// Global session registry used by DataFusion [`FunctionRegistry`] integration.
pub struct Registry {
    ctx: SessionContext,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
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
