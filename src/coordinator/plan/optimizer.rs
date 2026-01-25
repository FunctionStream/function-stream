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

use crate::coordinator::analyze::Analysis;
use crate::coordinator::plan::PlanNode;
use std::fmt;

pub trait PlanOptimizer: fmt::Debug + Send + Sync {
    fn optimize(&self, plan: Box<dyn PlanNode>, analysis: &Analysis) -> Box<dyn PlanNode>;

    fn name(&self) -> &str;
}

#[derive(Debug, Default)]
pub struct NoOpOptimizer;

impl PlanOptimizer for NoOpOptimizer {
    fn optimize(&self, plan: Box<dyn PlanNode>, _analysis: &Analysis) -> Box<dyn PlanNode> {
        plan
    }

    fn name(&self) -> &str {
        "NoOpOptimizer"
    }
}

#[derive(Debug)]
pub struct LogicalPlanner {
    optimizers: Vec<Box<dyn PlanOptimizer>>,
}

impl LogicalPlanner {
    pub fn new() -> Self {
        Self {
            optimizers: Vec::new(),
        }
    }

    pub fn with_optimizers(optimizers: Vec<Box<dyn PlanOptimizer>>) -> Self {
        Self { optimizers }
    }

    pub fn add_optimizer(&mut self, optimizer: Box<dyn PlanOptimizer>) {
        self.optimizers.push(optimizer);
    }

    pub fn optimize(&self, plan: Box<dyn PlanNode>, analysis: &Analysis) -> Box<dyn PlanNode> {
        let mut optimized_plan = plan;

        for optimizer in &self.optimizers {
            log::debug!("Applying optimizer: {}", optimizer.name());
            optimized_plan = optimizer.optimize(optimized_plan, analysis);
        }

        optimized_plan
    }
}

impl Default for LogicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}
