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

use datafusion::common::{DataFusionError, Result};
use protocol::grpc::api::EdgeType as ProtoEdgeType;
use serde::{Deserialize, Serialize};

use crate::sql::common::FsSchema;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogicalEdgeType {
    Forward,
    Shuffle,
    LeftJoin,
    RightJoin,
}

impl Display for LogicalEdgeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            LogicalEdgeType::Forward => "→",
            LogicalEdgeType::Shuffle => "⤨",
            LogicalEdgeType::LeftJoin => "-[left]⤨",
            LogicalEdgeType::RightJoin => "-[right]⤨",
        };
        write!(f, "{symbol}")
    }
}

impl From<ProtoEdgeType> for LogicalEdgeType {
    fn from(value: ProtoEdgeType) -> Self {
        match value {
            ProtoEdgeType::Unused => {
                panic!("Critical: Invalid EdgeType 'Unused' encountered")
            }
            ProtoEdgeType::Forward => Self::Forward,
            ProtoEdgeType::Shuffle => Self::Shuffle,
            ProtoEdgeType::LeftJoin => Self::LeftJoin,
            ProtoEdgeType::RightJoin => Self::RightJoin,
        }
    }
}

impl From<LogicalEdgeType> for ProtoEdgeType {
    fn from(value: LogicalEdgeType) -> Self {
        match value {
            LogicalEdgeType::Forward => Self::Forward,
            LogicalEdgeType::Shuffle => Self::Shuffle,
            LogicalEdgeType::LeftJoin => Self::LeftJoin,
            LogicalEdgeType::RightJoin => Self::RightJoin,
        }
    }
}

pub(crate) fn logical_edge_type_from_proto_i32(i: i32) -> Result<LogicalEdgeType> {
    let e = ProtoEdgeType::try_from(i).map_err(|_| {
        DataFusionError::Plan(format!("invalid protobuf EdgeType discriminant {i}"))
    })?;
    match e {
        ProtoEdgeType::Unused => Err(DataFusionError::Plan(
            "Critical: Invalid EdgeType 'Unused' encountered".into(),
        )),
        ProtoEdgeType::Forward => Ok(LogicalEdgeType::Forward),
        ProtoEdgeType::Shuffle => Ok(LogicalEdgeType::Shuffle),
        ProtoEdgeType::LeftJoin => Ok(LogicalEdgeType::LeftJoin),
        ProtoEdgeType::RightJoin => Ok(LogicalEdgeType::RightJoin),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
