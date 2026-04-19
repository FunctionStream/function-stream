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

use petgraph::prelude::*;
use petgraph::visit::NodeIndexable;
use tracing::debug;

use crate::sql::logical_node::logical::{LogicalEdgeType, LogicalGraph, Optimizer};

pub struct ChainingOptimizer {}

impl Optimizer for ChainingOptimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool {
        let mut match_found = None;

        for node_idx in plan.node_indices() {
            let mut outgoing = plan.edges_directed(node_idx, Outgoing);
            let first_out = outgoing.next();
            if first_out.is_none() || outgoing.next().is_some() {
                continue;
            }
            let edge = first_out.unwrap();

            if edge.weight().edge_type != LogicalEdgeType::Forward {
                continue;
            }

            let target_idx = edge.target();

            let mut incoming = plan.edges_directed(target_idx, Incoming);
            let first_in = incoming.next();
            if first_in.is_none() || incoming.next().is_some() {
                continue;
            }

            let source_node = plan.node_weight(node_idx).expect("Source node missing");
            let target_node = plan.node_weight(target_idx).expect("Target node missing");

            let parallelism_ok = source_node.parallelism == target_node.parallelism
                || target_node
                    .operator_chain
                    .is_parallelism_upstream_expandable();

            if source_node.operator_chain.is_source()
                || target_node.operator_chain.is_sink()
                || !parallelism_ok
            {
                continue;
            }

            match_found = Some((node_idx, target_idx, edge.id()));
            break;
        }

        if let Some((source_idx, target_idx, edge_id)) = match_found {
            let edge_weight = plan.remove_edge(edge_id).expect("Edge should exist");

            let target_outgoing: Vec<_> = plan
                .edges_directed(target_idx, Outgoing)
                .map(|e| (e.id(), e.target()))
                .collect();

            for (e_id, next_target_idx) in target_outgoing {
                let weight = plan.remove_edge(e_id).expect("Outgoing edge missing");
                plan.add_edge(source_idx, next_target_idx, weight);
            }

            let is_source_last = source_idx.index() == plan.node_bound() - 1;

            let target_node = plan
                .remove_node(target_idx)
                .expect("Target node should exist");

            let actual_source_idx = if is_source_last {
                target_idx
            } else {
                source_idx
            };

            let source_node = plan
                .node_weight_mut(actual_source_idx)
                .expect("Source node missing");

            debug!(
                "Chaining Optimizer: Fusing '{}' -> '{}'",
                source_node.description, target_node.description
            );

            source_node.description =
                format!("{} -> {}", source_node.description, target_node.description);

            source_node.parallelism = source_node.parallelism.max(target_node.parallelism);

            source_node
                .operator_chain
                .operators
                .extend(target_node.operator_chain.operators);
            source_node.operator_chain.edges.push(edge_weight.schema);

            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use crate::sql::common::FsSchema;
    use crate::sql::logical_node::logical::{
        LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName, Optimizer,
    };

    use super::ChainingOptimizer;

    fn forward_edge() -> LogicalEdge {
        let s = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        LogicalEdge::new(LogicalEdgeType::Forward, FsSchema::new_unkeyed(s, 0))
    }

    fn proj_node(id: u32, label: &str) -> LogicalNode {
        LogicalNode::single(
            id,
            format!("op_{label}"),
            OperatorName::Projection,
            vec![],
            label.to_string(),
            1,
        )
    }

    fn source_node() -> LogicalNode {
        LogicalNode::single(
            0,
            "src".into(),
            OperatorName::ConnectorSource,
            vec![],
            "source".into(),
            1,
        )
    }

    /// Window aggregate at higher default parallelism may forward into projection @ 1: still fuse
    /// so each branch does not reserve a separate global state-memory block for the same sub-chain.
    #[test]
    fn fusion_stateful_high_parallelism_into_expandable_low() {
        let mut g = LogicalGraph::new();
        let n0 = g.add_node(source_node());
        let n1 = g.add_node(proj_node(1, "tumble"));
        let n2 = g.add_node(proj_node(2, "proj"));
        let n1w = g.node_weight_mut(n1).unwrap();
        n1w.parallelism = 8;
        let e = forward_edge();
        g.add_edge(n0, n1, e.clone());
        g.add_edge(n1, n2, e);

        let changed = ChainingOptimizer {}.optimize_once(&mut g);
        assert!(changed);
        assert_eq!(g.node_count(), 2);
        let fused = g
            .node_weights()
            .find(|n| n.description.contains("->"))
            .unwrap();
        assert_eq!(fused.parallelism, 8);
        assert_eq!(fused.operator_chain.len(), 2);
    }

    /// Regression: upstream at last `NodeIndex` + remove non-last downstream swaps indices.
    #[test]
    fn fusion_remaps_when_upstream_was_last_node_index() {
        let mut g = LogicalGraph::new();
        let n0 = g.add_node(source_node());
        let n1 = g.add_node(proj_node(1, "downstream"));
        let n2 = g.add_node(proj_node(2, "upstream_last_index"));
        let e = forward_edge();
        g.add_edge(n0, n2, e.clone());
        g.add_edge(n2, n1, e);

        let changed = ChainingOptimizer {}.optimize_once(&mut g);
        assert!(changed);
        assert_eq!(g.node_count(), 2);
    }
}
