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

use std::sync::Arc;

use itertools::{EitherOrBoth, Itertools};
use prost::Message;
use protocol::grpc::api::ConnectorOp;
use serde::{Deserialize, Serialize};

use super::operator_name::OperatorName;
use crate::sql::common::FsSchema;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainedLogicalOperator {
    pub operator_id: String,
    pub operator_name: OperatorName,
    pub operator_config: Vec<u8>,
}

impl ChainedLogicalOperator {
    pub fn extract_connector_name(&self) -> Option<String> {
        if matches!(
            self.operator_name,
            OperatorName::ConnectorSource | OperatorName::ConnectorSink
        ) {
            ConnectorOp::decode(self.operator_config.as_slice())
                .ok()
                .map(|op| op.connector)
        } else {
            None
        }
    }

    pub fn extract_feature(&self) -> Option<String> {
        match self.operator_name {
            OperatorName::AsyncUdf => Some("async-udf".to_string()),
            OperatorName::Join => Some("join-with-expiration".to_string()),
            OperatorName::InstantJoin => Some("windowed-join".to_string()),
            OperatorName::WindowFunction => Some("sql-window-function".to_string()),
            OperatorName::LookupJoin => Some("lookup-join".to_string()),
            OperatorName::TumblingWindowAggregate => {
                Some("sql-tumbling-window-aggregate".to_string())
            }
            OperatorName::SlidingWindowAggregate => {
                Some("sql-sliding-window-aggregate".to_string())
            }
            OperatorName::SessionWindowAggregate => {
                Some("sql-session-window-aggregate".to_string())
            }
            OperatorName::UpdatingAggregate => Some("sql-updating-aggregate".to_string()),
            OperatorName::ConnectorSource => {
                self.extract_connector_name().map(|c| format!("{c}-source"))
            }
            OperatorName::ConnectorSink => {
                self.extract_connector_name().map(|c| format!("{c}-sink"))
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorChain {
    pub(crate) operators: Vec<ChainedLogicalOperator>,
    pub(crate) edges: Vec<Arc<FsSchema>>,
}

impl OperatorChain {
    pub fn new(operator: ChainedLogicalOperator) -> Self {
        Self {
            operators: vec![operator],
            edges: vec![],
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter()
            .zip_longest(&self.edges)
            .filter_map(|e| match e {
                EitherOrBoth::Both(op, edge) => Some((op, Some(edge))),
                EitherOrBoth::Left(op) => Some((op, None)),
                EitherOrBoth::Right(_) => None,
            })
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&mut ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter_mut()
            .zip_longest(&self.edges)
            .filter_map(|e| match e {
                EitherOrBoth::Both(op, edge) => Some((op, Some(edge))),
                EitherOrBoth::Left(op) => Some((op, None)),
                EitherOrBoth::Right(_) => None,
            })
    }

    pub fn first(&self) -> &ChainedLogicalOperator {
        self.operators
            .first()
            .expect("OperatorChain must contain at least one operator")
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    pub fn is_source(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSource
    }

    pub fn is_sink(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSink
    }
}
