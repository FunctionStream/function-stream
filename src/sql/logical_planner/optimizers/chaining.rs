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

use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};

use crate::sql::logical_node::logical::{LogicalEdgeType, LogicalGraph, Optimizer};

pub type NodeId = NodeIndex;
pub type EdgeId = EdgeIndex;

pub struct ChainingOptimizer;

impl ChainingOptimizer {
    fn find_fusion_candidate(plan: &LogicalGraph) -> Option<(NodeId, NodeId, EdgeId)> {
        let node_ids: Vec<NodeId> = plan.node_indices().collect();

        for upstream_id in node_ids {
            let upstream_node = plan.node_weight(upstream_id)?;

            if upstream_node.operator_chain.is_source() {
                continue;
            }

            let outgoing_edges: Vec<_> = plan.edges_directed(upstream_id, Outgoing).collect();

            if outgoing_edges.len() != 1 {
                continue;
            }

            let bridging_edge = &outgoing_edges[0];

            if bridging_edge.weight().edge_type != LogicalEdgeType::Forward {
                continue;
            }

            let downstream_id = bridging_edge.target();
            let downstream_node = plan.node_weight(downstream_id)?;

            if downstream_node.operator_chain.is_sink() {
                continue;
            }

            if upstream_node.parallelism != downstream_node.parallelism {
                continue;
            }

            let incoming_edges: Vec<_> = plan.edges_directed(downstream_id, Incoming).collect();
            if incoming_edges.len() != 1 {
                continue;
            }

            return Some((upstream_id, downstream_id, bridging_edge.id()));
        }

        None
    }

    fn apply_fusion(
        plan: &mut LogicalGraph,
        upstream_id: NodeId,
        downstream_id: NodeId,
        bridging_edge_id: EdgeId,
    ) {
        let bridging_edge = plan
            .remove_edge(bridging_edge_id)
            .expect("Graph Integrity Violation: Bridging edge missing");

        let propagated_schema = bridging_edge.schema.clone();

        let downstream_outgoing: Vec<_> = plan
            .edges_directed(downstream_id, Outgoing)
            .map(|e| (e.id(), e.target()))
            .collect();

        for (edge_id, target_id) in downstream_outgoing {
            let edge_weight = plan
                .remove_edge(edge_id)
                .expect("Graph Integrity Violation: Outgoing edge missing");

            plan.add_edge(upstream_id, target_id, edge_weight);
        }

        let downstream_node = plan
            .remove_node(downstream_id)
            .expect("Graph Integrity Violation: Downstream node missing");

        let upstream_node = plan
            .node_weight_mut(upstream_id)
            .expect("Graph Integrity Violation: Upstream node missing");

        upstream_node.description = format!(
            "{} -> {}",
            upstream_node.description, downstream_node.description
        );

        upstream_node
            .operator_chain
            .operators
            .extend(downstream_node.operator_chain.operators);

        upstream_node
            .operator_chain
            .edges
            .push(propagated_schema);
    }
}

impl Optimizer for ChainingOptimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool {
        if let Some((upstream_id, downstream_id, bridging_edge_id)) =
            Self::find_fusion_candidate(plan)
        {
            Self::apply_fusion(plan, upstream_id, downstream_id, bridging_edge_id);
            true
        } else {
            false
        }
    }
}
