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

use crate::sql::logical_node::logical::{LogicalEdge, LogicalNode};
use crate::sql::logical_planner::planner::{NamedNode, Planner};
use crate::sql::common::{FsSchema, FsSchemaRef};

pub(crate) trait StreamExtension: Debug {
    fn node_name(&self) -> Option<NamedNode>;
    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<FsSchemaRef>,
    ) -> Result<NodeWithIncomingEdges>;
    fn output_schema(&self) -> FsSchema;
    fn transparent(&self) -> bool {
        false
    }
}

pub(crate) struct NodeWithIncomingEdges {
    pub node: LogicalNode,
    pub edges: Vec<LogicalEdge>,
}
