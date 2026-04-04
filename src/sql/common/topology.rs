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

//! EXPLAIN-like DAG text renderer for [`FsProgram`].
//!
//! Renders a streaming pipeline topology as a human-readable ASCII graph using
//! Kahn's topological sort.  Handles linear chains, fan-out, and fan-in (JOIN).

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;

use protocol::grpc::api::FsProgram;

fn edge_type_label(edge_type: i32) -> &'static str {
    match edge_type {
        1 => "Forward",
        2 => "Shuffle",
        3 => "LeftJoin",
        4 => "RightJoin",
        _ => "Unknown",
    }
}

/// Render an [`FsProgram`] as an EXPLAIN-style topology string.
pub fn render_program_topology(program: &FsProgram) -> String {
    if program.nodes.is_empty() {
        return "(empty topology)".to_string();
    }

    struct EdgeInfo {
        target: i32,
        edge_type: i32,
    }
    struct InputInfo {
        source: i32,
        edge_type: i32,
    }

    let node_map: BTreeMap<i32, &protocol::grpc::api::FsNode> =
        program.nodes.iter().map(|n| (n.node_index, n)).collect();

    let mut downstream: BTreeMap<i32, Vec<EdgeInfo>> = BTreeMap::new();
    let mut upstream: BTreeMap<i32, Vec<InputInfo>> = BTreeMap::new();
    let mut in_degree: BTreeMap<i32, usize> = BTreeMap::new();

    for idx in node_map.keys() {
        in_degree.entry(*idx).or_insert(0);
    }
    for edge in &program.edges {
        downstream.entry(edge.source).or_default().push(EdgeInfo {
            target: edge.target,
            edge_type: edge.edge_type,
        });
        upstream.entry(edge.target).or_default().push(InputInfo {
            source: edge.source,
            edge_type: edge.edge_type,
        });
        *in_degree.entry(edge.target).or_insert(0) += 1;
    }

    // Kahn's topological sort
    let mut queue: VecDeque<i32> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(idx, _)| *idx)
        .collect();
    let mut topo_order: Vec<i32> = Vec::with_capacity(node_map.len());
    let mut remaining = in_degree.clone();
    while let Some(idx) = queue.pop_front() {
        topo_order.push(idx);
        if let Some(edges) = downstream.get(&idx) {
            for e in edges {
                if let Some(deg) = remaining.get_mut(&e.target) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(e.target);
                    }
                }
            }
        }
    }
    for idx in node_map.keys() {
        if !topo_order.contains(idx) {
            topo_order.push(*idx);
        }
    }

    let is_source = |idx: &i32| upstream.get(idx).is_none_or(|v| v.is_empty());
    let is_sink = |idx: &i32| downstream.get(idx).is_none_or(|v| v.is_empty());

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Pipeline Topology  ({} nodes, {} edges)",
        program.nodes.len(),
        program.edges.len(),
    );
    let _ = writeln!(out, "{}", "=".repeat(50));

    for (pos, &node_idx) in topo_order.iter().enumerate() {
        let Some(node) = node_map.get(&node_idx) else {
            continue;
        };

        let op_chain: String = node
            .operators
            .iter()
            .map(|op| op.operator_name.as_str())
            .collect::<Vec<_>>()
            .join(" -> ");

        let role = if is_source(&node_idx) {
            "Source"
        } else if is_sink(&node_idx) {
            "Sink"
        } else {
            "Operator"
        };

        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "[{role}] Node {node_idx}    parallelism = {}",
            node.parallelism,
        );
        let _ = writeln!(out, "  operators:  {op_chain}");

        if !node.description.is_empty() {
            let _ = writeln!(out, "  desc:       {}", node.description);
        }

        if let Some(inputs) = upstream.get(&node_idx) {
            if inputs.len() == 1 {
                let i = &inputs[0];
                let _ = writeln!(
                    out,
                    "  input:      <-- [{}] Node {}",
                    edge_type_label(i.edge_type),
                    i.source,
                );
            } else if inputs.len() > 1 {
                let _ = writeln!(out, "  inputs:");
                for i in inputs {
                    let _ = writeln!(
                        out,
                        "              <-- [{}] Node {}",
                        edge_type_label(i.edge_type),
                        i.source,
                    );
                }
            }
        }

        if let Some(outputs) = downstream.get(&node_idx) {
            if outputs.len() == 1 {
                let e = &outputs[0];
                let _ = writeln!(
                    out,
                    "  output:     --> [{}] Node {}",
                    edge_type_label(e.edge_type),
                    e.target,
                );
            } else if outputs.len() > 1 {
                let _ = writeln!(out, "  outputs:");
                for e in outputs {
                    let _ = writeln!(
                        out,
                        "              --> [{}] Node {}",
                        edge_type_label(e.edge_type),
                        e.target,
                    );
                }
            }
        }

        if pos < topo_order.len() - 1 {
            let single_out = downstream.get(&node_idx).is_some_and(|v| v.len() == 1);
            let next_idx = topo_order.get(pos + 1).copied();
            let is_direct = single_out
                && next_idx
                    .is_some_and(|n| downstream.get(&node_idx).is_some_and(|v| v[0].target == n));
            let next_single_in = next_idx
                .and_then(|n| upstream.get(&n))
                .is_some_and(|v| v.len() == 1);

            if is_direct && next_single_in {
                let etype = downstream.get(&node_idx).unwrap()[0].edge_type;
                let _ = writeln!(out, "        |");
                let _ = writeln!(out, "        | {}", edge_type_label(etype));
                let _ = writeln!(out, "        v");
            }
        }
    }

    out.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::grpc::api::{ChainedOperator, FsEdge, FsNode, FsProgram};

    fn make_node(
        node_index: i32,
        operators: Vec<(&str, &str)>,
        desc: &str,
        parallelism: u32,
    ) -> FsNode {
        FsNode {
            node_index,
            node_id: node_index as u32,
            parallelism,
            description: desc.to_string(),
            operators: operators
                .into_iter()
                .map(|(id, name)| ChainedOperator {
                    operator_id: id.to_string(),
                    operator_name: name.to_string(),
                    operator_config: Vec::new(),
                })
                .collect(),
            edges: Vec::new(),
        }
    }

    fn make_edge(source: i32, target: i32, edge_type: i32) -> FsEdge {
        FsEdge {
            source,
            target,
            schema: None,
            edge_type,
        }
    }

    #[test]
    fn empty_program_renders_placeholder() {
        let program = FsProgram {
            nodes: vec![],
            edges: vec![],
            program_config: None,
        };
        assert_eq!(render_program_topology(&program), "(empty topology)");
    }

    #[test]
    fn linear_pipeline_renders_correctly() {
        let program = FsProgram {
            nodes: vec![
                make_node(0, vec![("src_0", "ConnectorSource")], "", 1),
                make_node(
                    1,
                    vec![("val_1", "Value"), ("wm_2", "ExpressionWatermark")],
                    "source -> watermark",
                    1,
                ),
                make_node(2, vec![("sink_3", "ConnectorSink")], "sink (kafka)", 1),
            ],
            edges: vec![make_edge(0, 1, 1), make_edge(1, 2, 1)],
            program_config: None,
        };
        let result = render_program_topology(&program);
        assert!(result.contains("[Source] Node 0"));
        assert!(result.contains("[Operator] Node 1"));
        assert!(result.contains("[Sink] Node 2"));
        assert!(result.contains("ConnectorSource"));
        assert!(result.contains("Value -> ExpressionWatermark"));
        assert!(result.contains("Forward"));
    }

    #[test]
    fn join_topology_shows_multiple_inputs() {
        let program = FsProgram {
            nodes: vec![
                make_node(0, vec![("src_a", "ConnectorSource")], "source A", 1),
                make_node(1, vec![("src_b", "ConnectorSource")], "source B", 1),
                make_node(2, vec![("join_0", "WindowJoin")], "join node", 2),
                make_node(3, vec![("sink_0", "ConnectorSink")], "sink", 1),
            ],
            edges: vec![
                make_edge(0, 2, 3), // LeftJoin
                make_edge(1, 2, 4), // RightJoin
                make_edge(2, 3, 1), // Forward
            ],
            program_config: None,
        };
        let result = render_program_topology(&program);
        assert!(result.contains("inputs:"));
        assert!(result.contains("LeftJoin"));
        assert!(result.contains("RightJoin"));
        assert!(result.contains("[Operator] Node 2"));
    }
}
