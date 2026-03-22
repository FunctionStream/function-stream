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

//! Conversions between [`LogicalProgram`] and `protocol::grpc::api::FsProgram` / pipeline API types.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DFResult};
use petgraph::graph::DiGraph;
use petgraph::prelude::EdgeRef;
use protocol::grpc::api::{
    ChainedOperator, EdgeType as ProtoEdgeType, FsEdge, FsNode, FsProgram, FsSchema as ProtoFsSchema,
};

use crate::sql::api::pipelines::{PipelineEdge, PipelineGraph, PipelineNode};
use crate::sql::common::FsSchema;

use super::logical_edge::logical_edge_type_from_proto_i32;
use super::operator_chain::{ChainedLogicalOperator, OperatorChain};
use super::operator_name::OperatorName;
use super::{LogicalEdge, LogicalNode, LogicalProgram, ProgramConfig};

impl TryFrom<FsProgram> for LogicalProgram {
    type Error = DataFusionError;

    fn try_from(value: FsProgram) -> DFResult<Self> {
        let mut graph = DiGraph::new();
        let mut id_map = HashMap::with_capacity(value.nodes.len());

        for node in value.nodes {
            let operators = node
                .operators
                .into_iter()
                .map(|op| {
                    let ChainedOperator {
                        operator_id,
                        operator_name: name_str,
                        operator_config,
                    } = op;
                    let operator_name = OperatorName::from_str(&name_str).map_err(|_| {
                        DataFusionError::Plan(format!("Invalid operator name: {name_str}"))
                    })?;
                    Ok(ChainedLogicalOperator {
                        operator_id,
                        operator_name,
                        operator_config,
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            let edges = node
                .edges
                .into_iter()
                .map(|e| {
                    let fs: FsSchema = e.try_into()?;
                    Ok(Arc::new(fs))
                })
                .collect::<DFResult<Vec<_>>>()?;

            let logical_node = LogicalNode {
                node_id: node.node_id,
                description: node.description,
                operator_chain: OperatorChain { operators, edges },
                parallelism: node.parallelism as usize,
            };

            id_map.insert(node.node_index, graph.add_node(logical_node));
        }

        for edge in value.edges {
            let source = *id_map.get(&edge.source).ok_or_else(|| {
                DataFusionError::Plan("Graph integrity error: Missing source node".into())
            })?;
            let target = *id_map.get(&edge.target).ok_or_else(|| {
                DataFusionError::Plan("Graph integrity error: Missing target node".into())
            })?;
            let schema = edge
                .schema
                .ok_or_else(|| DataFusionError::Plan("Graph integrity error: Missing edge schema".into()))?;
            let edge_type = logical_edge_type_from_proto_i32(edge.edge_type)?;

            graph.add_edge(
                source,
                target,
                LogicalEdge {
                    edge_type,
                    schema: Arc::new(FsSchema::try_from(schema)?),
                },
            );
        }

        let program_config = value
            .program_config
            .map(ProgramConfig::from)
            .unwrap_or_default();

        Ok(LogicalProgram::new(graph, program_config))
    }
}

impl From<LogicalProgram> for FsProgram {
    fn from(value: LogicalProgram) -> Self {
        let nodes = value
            .graph
            .node_indices()
            .filter_map(|idx| value.graph.node_weight(idx).map(|node| (idx, node)))
            .map(|(idx, node)| FsNode {
                node_index: idx.index() as i32,
                node_id: node.node_id,
                parallelism: node.parallelism as u32,
                description: node.description.clone(),
                operators: node
                    .operator_chain
                    .operators
                    .iter()
                    .map(|op| ChainedOperator {
                        operator_id: op.operator_id.clone(),
                        operator_name: op.operator_name.to_string(),
                        operator_config: op.operator_config.clone(),
                    })
                    .collect(),
                edges: node
                    .operator_chain
                    .edges
                    .iter()
                    .map(|edge| ProtoFsSchema::from((**edge).clone()))
                    .collect(),
            })
            .collect();

        let edges = value
            .graph
            .edge_indices()
            .filter_map(|eidx| {
                let edge = value.graph.edge_weight(eidx)?;
                let (source, target) = value.graph.edge_endpoints(eidx)?;
                Some(FsEdge {
                    source: source.index() as i32,
                    target: target.index() as i32,
                    schema: Some(ProtoFsSchema::from((*edge.schema).clone())),
                    edge_type: ProtoEdgeType::from(edge.edge_type) as i32,
                })
            })
            .collect();

        FsProgram {
            nodes,
            edges,
            program_config: Some(value.program_config.into()),
        }
    }
}

impl TryFrom<LogicalProgram> for PipelineGraph {
    type Error = DataFusionError;

    fn try_from(value: LogicalProgram) -> DFResult<Self> {
        let nodes = value
            .graph
            .node_weights()
            .map(|node| {
                Ok(PipelineNode {
                    node_id: node.node_id,
                    operator: node.resolve_pipeline_operator_name()?,
                    description: node.description.clone(),
                    parallelism: node.parallelism as u32,
                })
            })
            .collect::<DFResult<Vec<_>>>()?;

        let edges = value
            .graph
            .edge_references()
            .filter_map(|edge| {
                let src = value.graph.node_weight(edge.source())?;
                let target = value.graph.node_weight(edge.target())?;
                Some(PipelineEdge {
                    src_id: src.node_id,
                    dest_id: target.node_id,
                    key_type: "()".to_string(),
                    value_type: "()".to_string(),
                    edge_type: format!("{:?}", edge.weight().edge_type),
                })
            })
            .collect();

        Ok(PipelineGraph { nodes, edges })
    }
}
