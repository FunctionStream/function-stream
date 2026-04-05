// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result as DFResult};
use petgraph::Direction;
use petgraph::dot::Dot;
use prost::Message;
use protocol::function_stream_graph::FsProgram;
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

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
            if let Some(&p) = overrides.get(&node.node_id) {
                node.parallelism = p;
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
            .filter_map(|idx| self.graph.node_weight(idx))
            .map(|node| node.node_id)
            .collect()
    }

    pub fn get_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let program_bytes = FsProgram::from(self.clone()).encode_to_vec();
        hasher.write(&program_bytes);
        let rng = SmallRng::seed_from_u64(hasher.finish());
        rng.sample_iter(&Alphanumeric)
            .take(16)
            .map(|c| (c as char).to_ascii_lowercase())
            .collect()
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        self.graph
            .node_weights()
            .flat_map(|node| {
                node.operator_chain
                    .operators
                    .iter()
                    .map(move |op| (op.operator_id.clone(), node.parallelism))
            })
            .collect()
    }

    pub fn operator_names_by_id(&self) -> HashMap<String, String> {
        self.graph
            .node_weights()
            .flat_map(|node| &node.operator_chain.operators)
            .map(|op| {
                let resolved_name = op
                    .extract_connector_name()
                    .unwrap_or_else(|| op.operator_name.to_string());
                (op.operator_id.clone(), resolved_name)
            })
            .collect()
    }

    pub fn tasks_per_node(&self) -> HashMap<u32, usize> {
        self.graph
            .node_weights()
            .map(|node| (node.node_id, node.parallelism))
            .collect()
    }

    pub fn features(&self) -> HashSet<String> {
        self.graph
            .node_weights()
            .flat_map(|node| &node.operator_chain.operators)
            .filter_map(|op| op.extract_feature())
            .collect()
    }

    /// Arrow schema carried on edges into the connector-sink node, if present.
    pub fn egress_arrow_schema(&self) -> Option<Arc<Schema>> {
        for idx in self.graph.node_indices() {
            let node = self.graph.node_weight(idx)?;
            if node
                .operator_chain
                .operators
                .iter()
                .any(|op| op.operator_name == OperatorName::ConnectorSink)
            {
                let e = self.graph.edges_directed(idx, Direction::Incoming).next()?;
                return Some(Arc::clone(&e.weight().schema.schema));
            }
        }
        None
    }

    pub fn encode_for_catalog(&self) -> DFResult<Vec<u8>> {
        Ok(FsProgram::from(self.clone()).encode_to_vec())
    }

    pub fn decode_for_catalog(bytes: &[u8]) -> DFResult<Self> {
        let proto = FsProgram::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("FsProgram catalog decode failed: {e}"))
        })?;
        LogicalProgram::try_from(proto)
    }
}
