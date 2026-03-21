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

use itertools::Itertools;

use super::operator_name::OperatorName;
use crate::sql::common::FsSchema;

#[derive(Clone, Debug)]
pub struct ChainedLogicalOperator {
    pub operator_id: String,
    pub operator_name: OperatorName,
    pub operator_config: Vec<u8>,
}

#[derive(Clone, Debug)]
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

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&mut ChainedLogicalOperator, Option<&Arc<FsSchema>>)> {
        self.operators
            .iter_mut()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn first(&self) -> &ChainedLogicalOperator {
        &self.operators[0]
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
