use std::collections::HashMap;
use anyhow::Result;

use crate::runtime::streaming::cluster::graph::{
    ExchangeMode, ExecutionGraph, JobId, OperatorUid, PartitioningStrategy,
    PhysicalEdgeDescriptor, ResourceProfile, SubtaskIndex, TaskDeploymentDescriptor, VertexId,
};

use arroyo_datastream::logical::{LogicalEdgeType, LogicalGraph, OperatorChain};
use petgraph::Direction;
use sha2::{Digest, Sha256};
use crate::sql::logical_node::logical::{LogicalEdgeType, LogicalGraph};

#[derive(thiserror::Error, Debug)]
pub enum CompileError {
    #[error("Topology Error: Forward edge between Vertex {src} (p={src_p}) and {dst} (p={dst_p}) requires identical parallelism.")]
    ParallelismMismatch {
        src: u32,
        src_p: usize,
        dst: u32,
        dst_p: usize,
    },

    #[error("Serialization Error: Failed to serialize operator chain for Vertex {vertex_id}. Error: {source}")]
    SerializationFailed {
        vertex_id: u32,
        source: anyhow::Error,
    },

    #[error("Validation Error: {0}")]
    ValidationError(String),
}

pub struct JobCompiler;

impl JobCompiler {
    pub fn compile(
        job_id: String,
        logical: &LogicalGraph,
    ) -> Result<ExecutionGraph, CompileError> {
        let mut tasks = Vec::new();
        let mut edges = Vec::new();
        let job_id_typed = JobId(job_id.clone());

        // ====================================================================
        // 阶段 1：预计算网络门数量 (Pre-compute Network Gates)
        // ====================================================================
        let mut in_degrees: HashMap<(u32, u32), usize> = HashMap::new();
        let mut out_degrees: HashMap<(u32, u32), usize> = HashMap::new();

        for edge_idx in logical.edge_indices() {
            let edge = logical.edge_weight(edge_idx).unwrap();
            let (src_idx, dst_idx) = logical.edge_endpoints(edge_idx).unwrap();
            let src_node = logical.node_weight(src_idx).unwrap();
            let dst_node = logical.node_weight(dst_idx).unwrap();

            match edge.edge_type {
                LogicalEdgeType::Forward => {
                    if src_node.parallelism != dst_node.parallelism {
                        return Err(CompileError::ParallelismMismatch {
                            src: src_node.node_id,
                            src_p: src_node.parallelism,
                            dst: dst_node.node_id,
                            dst_p: dst_node.parallelism,
                        });
                    }
                    for i in 0..src_node.parallelism as u32 {
                        *out_degrees.entry((src_node.node_id, i)).or_insert(0) += 1;
                        *in_degrees.entry((dst_node.node_id, i)).or_insert(0) += 1;
                    }
                }
                LogicalEdgeType::Shuffle
                | LogicalEdgeType::LeftJoin
                | LogicalEdgeType::RightJoin => {
                    for s in 0..src_node.parallelism as u32 {
                        *out_degrees.entry((src_node.node_id, s)).or_insert(0) +=
                            dst_node.parallelism;
                    }
                    for d in 0..dst_node.parallelism as u32 {
                        *in_degrees.entry((dst_node.node_id, d)).or_insert(0) +=
                            src_node.parallelism;
                    }
                }
            }
        }

        // ====================================================================
        // 阶段 2：节点展开与算子融合 (Node Expansion & Operator Fusion)
        // ====================================================================
        for idx in logical.node_indices() {
            let node = logical.node_weight(idx).unwrap();
            let parallelism = node.parallelism as u32;

            let in_schemas: Vec<_> = logical
                .edges_directed(idx, Direction::Incoming)
                .map(|e| e.weight().schema.clone())
                .collect();
            let out_schema = logical
                .edges_directed(idx, Direction::Outgoing)
                .map(|e| e.weight().schema.clone())
                .next();

            let is_source = node.operator_chain.is_source();
            let (head_op, _) = node
                .operator_chain
                .iter()
                .next()
                .expect("operator chain is non-empty");

            let chain_payload =
                Self::serialize_operator_chain(&node.operator_chain).map_err(|e| {
                    CompileError::SerializationFailed {
                        vertex_id: node.node_id,
                        source: e,
                    }
                })?;

            let base_uid = Self::generate_deterministic_uid(
                &job_id,
                node.node_id,
                &node.operator_chain,
            );

            let resource_profile =
                Self::calculate_resource_profile(&node.operator_chain, parallelism);

            for subtask_idx in 0..parallelism {
                let s_idx = SubtaskIndex(subtask_idx);
                let v_id = VertexId(node.node_id);

                let input_gates_count = *in_degrees
                    .get(&(node.node_id, subtask_idx))
                    .unwrap_or(&0);
                let output_gates_count = *out_degrees
                    .get(&(node.node_id, subtask_idx))
                    .unwrap_or(&0);

                tasks.push(TaskDeploymentDescriptor {
                    job_id: job_id_typed.clone(),
                    vertex_id: v_id,
                    subtask_idx: s_idx,
                    parallelism,
                    operator_name: head_op.operator_name.to_string(),
                    operator_uid: OperatorUid(format!("{}-{}", base_uid, subtask_idx)),
                    is_source,
                    operator_config_payload: chain_payload.clone(),
                    resources: resource_profile.clone(),
                    in_schemas: in_schemas.clone(),
                    out_schema: out_schema.clone(),
                    input_gates_count,
                    output_gates_count,
                });
            }
        }

        // ====================================================================
        // 阶段 3：物理边展开与路由策略推断 (Edge Expansion & Partitioning)
        // ====================================================================
        for edge_idx in logical.edge_indices() {
            let edge = logical.edge_weight(edge_idx).unwrap();
            let (src_graph_idx, dst_graph_idx) = logical.edge_endpoints(edge_idx).unwrap();
            let src_node = logical.node_weight(src_graph_idx).unwrap();
            let dst_node = logical.node_weight(dst_graph_idx).unwrap();

            let partitioning = match edge.edge_type {
                LogicalEdgeType::Forward => PartitioningStrategy::Forward,
                LogicalEdgeType::Shuffle
                | LogicalEdgeType::LeftJoin
                | LogicalEdgeType::RightJoin => {
                    if let Some(key_indices) = edge.schema.key_indices.as_ref() {
                        if !key_indices.is_empty() {
                            PartitioningStrategy::HashByKeys(key_indices.clone())
                        } else {
                            PartitioningStrategy::Rebalance
                        }
                    } else {
                        PartitioningStrategy::Rebalance
                    }
                }
            };

            let default_exchange = ExchangeMode::LocalThread;

            match edge.edge_type {
                LogicalEdgeType::Forward => {
                    for i in 0..src_node.parallelism as u32 {
                        edges.push(PhysicalEdgeDescriptor {
                            src_vertex: VertexId(src_node.node_id),
                            src_subtask: SubtaskIndex(i),
                            dst_vertex: VertexId(dst_node.node_id),
                            dst_subtask: SubtaskIndex(i),
                            partitioning: partitioning.clone(),
                            exchange_mode: default_exchange.clone(),
                        });
                    }
                }
                _ => {
                    for src_idx in 0..src_node.parallelism as u32 {
                        for dst_idx in 0..dst_node.parallelism as u32 {
                            edges.push(PhysicalEdgeDescriptor {
                                src_vertex: VertexId(src_node.node_id),
                                src_subtask: SubtaskIndex(src_idx),
                                dst_vertex: VertexId(dst_node.node_id),
                                dst_subtask: SubtaskIndex(dst_idx),
                                partitioning: partitioning.clone(),
                                exchange_mode: default_exchange.clone(),
                            });
                        }
                    }
                }
            }
        }

        let exec_graph = ExecutionGraph {
            job_id: job_id_typed,
            tasks,
            edges,
        };

        // ====================================================================
        // 阶段 4：执行拓扑图防御性自检 (Validation)
        // ====================================================================
        exec_graph
            .validate()
            .map_err(CompileError::ValidationError)?;

        Ok(exec_graph)
    }

    /// 确定性状态 UID 生成器：哪怕拓扑变化，只要算子内部逻辑不变就能继承状态。
    fn generate_deterministic_uid(
        job_id: &str,
        node_id: u32,
        chain: &OperatorChain,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(job_id.as_bytes());
        hasher.update(&node_id.to_le_bytes());

        for (op, _) in chain.iter() {
            hasher.update(op.operator_name.to_string().as_bytes());
            hasher.update(&op.operator_config);
        }

        let result = hasher.finalize();
        hex::encode(&result[..8])
    }

    /// 序列化整条算子链 (Operator Fusion)
    fn serialize_operator_chain(chain: &OperatorChain) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(chain, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("bincode encode failed: {}", e))
    }

    /// 资源画像智能推算
    fn calculate_resource_profile(
        chain: &OperatorChain,
        parallelism: u32,
    ) -> ResourceProfile {
        let mut profile = ResourceProfile::default();

        for (op, _) in chain.iter() {
            let name = op.operator_name.to_string();
            if name.contains("Window") || name.contains("Join") || name.contains("Aggregate") {
                profile.managed_memory_bytes += 512 * 1024 * 1024 / parallelism as u64;
                profile.cpu_cores += 0.5;
            }
            if name.contains("Source") || name.contains("Sink") {
                profile.network_memory_bytes += 128 * 1024 * 1024 / parallelism as u64;
            }
        }
        profile
    }
}
