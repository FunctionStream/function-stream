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

use std::fmt::Debug;

use datafusion::common::Result;

use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalNode};
use crate::sql::logical_planner::planner::{NamedNode, Planner};

// -----------------------------------------------------------------------------
// Core Execution Blueprint
// -----------------------------------------------------------------------------

/// Atomic unit within a streaming execution topology: translates streaming SQL into graph nodes.
pub(crate) trait StreamingOperatorBlueprint: Debug {
    /// Canonical named identity for this operator, if any (sources, sinks, etc.).
    fn operator_identity(&self) -> Option<NamedNode>;

    /// Compiles this operator into a graph vertex and its incoming routing edges.
    fn compile_to_graph_node(
        &self,
        compiler_context: &Planner,
        node_id_sequence: usize,
        upstream_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode>;

    /// Schema of records this operator yields downstream.
    fn yielded_schema(&self) -> FsSchema;

    /// Logical passthrough boundary (no physical state change); default is stateful / materializing.
    fn is_passthrough_boundary(&self) -> bool {
        false
    }
}

// -----------------------------------------------------------------------------
// Graph Topology Structures
// -----------------------------------------------------------------------------

/// Compiled vertex: execution unit plus upstream routing edges.
#[derive(Debug, Clone)]
pub(crate) struct CompiledTopologyNode {
    pub execution_unit: LogicalNode,
    pub routing_edges: Vec<LogicalEdge>,
}

impl CompiledTopologyNode {
    pub fn new(execution_unit: LogicalNode, routing_edges: Vec<LogicalEdge>) -> Self {
        Self {
            execution_unit,
            routing_edges,
        }
    }
}
