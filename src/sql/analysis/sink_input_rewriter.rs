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

use crate::sql::extensions::sink::StreamEgressNode;
use crate::sql::extensions::StreamingOperatorBlueprint;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use std::collections::HashMap;
use std::sync::Arc;
use crate::sql::logical_planner::planner::NamedNode;

type SinkInputs = HashMap<NamedNode, Vec<LogicalPlan>>;

/// Merges inputs for sinks with the same name to avoid duplicate sinks in the plan.
pub struct SinkInputRewriter<'a> {
    sink_inputs: &'a mut SinkInputs,
    pub was_removed: bool,
}

impl<'a> SinkInputRewriter<'a> {
    pub fn new(sink_inputs: &'a mut SinkInputs) -> Self {
        Self {
            sink_inputs,
            was_removed: false,
        }
    }
}

impl TreeNodeRewriter for SinkInputRewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let LogicalPlan::Extension(extension) = &node {
            if let Some(sink_node) = extension.node.as_any().downcast_ref::<StreamEgressNode>() {
                if let Some(named_node) = sink_node.operator_identity() {
                    if let Some(inputs) = self.sink_inputs.remove(&named_node) {
                        let new_node = LogicalPlan::Extension(Extension {
                            node: Arc::new(sink_node.with_exprs_and_inputs(vec![], inputs)?),
                        });
                        return Ok(Transformed::new(new_node, true, TreeNodeRecursion::Jump));
                    } else {
                        self.was_removed = true;
                    }
                }
            }
        }
        Ok(Transformed::no(node))
    }
}
