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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::sql::common::FsSchema;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum LogicalEdgeType {
    Forward,
    Shuffle,
    LeftJoin,
    RightJoin,
}

impl Display for LogicalEdgeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalEdgeType::Forward => write!(f, "→"),
            LogicalEdgeType::Shuffle => write!(f, "⤨"),
            LogicalEdgeType::LeftJoin => write!(f, "-[left]⤨"),
            LogicalEdgeType::RightJoin => write!(f, "-[right]⤨"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEdge {
    pub edge_type: LogicalEdgeType,
    pub schema: Arc<FsSchema>,
}

impl LogicalEdge {
    pub fn new(edge_type: LogicalEdgeType, schema: FsSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }

    pub fn project_all(edge_type: LogicalEdgeType, schema: FsSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }
}
