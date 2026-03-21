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

use std::collections::{HashMap, HashSet};

use petgraph::Direction;
use petgraph::dot::Dot;

use super::logical_graph::{LogicalGraph, Optimizer};
use super::operator_name::OperatorName;
use super::program_config::ProgramConfig;

#[derive(Clone, Debug, Default)]
pub struct LogicalProgram {
    pub graph: LogicalGraph,
    pub program_config: ProgramConfig,
}

impl LogicalProgram {
    pub fn new(graph: LogicalGraph, program_config: ProgramConfig) -> Self {
        Self {
            graph,
            program_config,
        }
    }

    pub fn optimize(&mut self, optimizer: &dyn Optimizer) {
        optimizer.optimize(&mut self.graph);
    }

    pub fn update_parallelism(&mut self, overrides: &HashMap<u32, usize>) {
        for node in self.graph.node_weights_mut() {
            if let Some(p) = overrides.get(&node.node_id) {
                node.parallelism = *p;
            }
        }
    }

    pub fn dot(&self) -> String {
        format!("{:?}", Dot::with_config(&self.graph, &[]))
    }

    pub fn task_count(&self) -> usize {
        self.graph.node_weights().map(|nw| nw.parallelism).sum()
    }

    pub fn sources(&self) -> HashSet<u32> {
        self.graph
            .externals(Direction::Incoming)
            .map(|t| self.graph.node_weight(t).unwrap().node_id)
            .collect()
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                tasks_per_operator.insert(op.operator_id.clone(), node.parallelism);
            }
        }
        tasks_per_operator
    }

    pub fn operator_names_by_id(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                m.insert(op.operator_id.clone(), op.operator_name.to_string());
            }
        }
        m
    }

    pub fn tasks_per_node(&self) -> HashMap<u32, usize> {
        let mut tasks_per_node = HashMap::new();
        for node in self.graph.node_weights() {
            tasks_per_node.insert(node.node_id, node.parallelism);
        }
        tasks_per_node
    }

    pub fn features(&self) -> HashSet<String> {
        let mut s = HashSet::new();
        for n in self.graph.node_weights() {
            for t in &n.operator_chain.operators {
                let feature = match &t.operator_name {
                    OperatorName::AsyncUdf => "async-udf".to_string(),
                    OperatorName::ExpressionWatermark
                    | OperatorName::ArrowValue
                    | OperatorName::ArrowKey
                    | OperatorName::Projection => continue,
                    OperatorName::Join => "join-with-expiration".to_string(),
                    OperatorName::InstantJoin => "windowed-join".to_string(),
                    OperatorName::WindowFunction => "sql-window-function".to_string(),
                    OperatorName::LookupJoin => "lookup-join".to_string(),
                    OperatorName::TumblingWindowAggregate => {
                        "sql-tumbling-window-aggregate".to_string()
                    }
                    OperatorName::SlidingWindowAggregate => {
                        "sql-sliding-window-aggregate".to_string()
                    }
                    OperatorName::SessionWindowAggregate => {
                        "sql-session-window-aggregate".to_string()
                    }
                    OperatorName::UpdatingAggregate => "sql-updating-aggregate".to_string(),
                    OperatorName::ConnectorSource => "connector-source".to_string(),
                    OperatorName::ConnectorSink => "connector-sink".to_string(),
                };
                s.insert(feature);
            }
        }
        s
    }
}
