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

use std::fmt::{Debug, Display, Formatter};

use super::operator_chain::{ChainedLogicalOperator, OperatorChain};
use super::operator_name::OperatorName;

#[derive(Clone)]
pub struct LogicalNode {
    pub node_id: u32,
    pub description: String,
    pub operator_chain: OperatorChain,
    pub parallelism: usize,
}

impl LogicalNode {
    pub fn single(
        id: u32,
        operator_id: String,
        name: OperatorName,
        config: Vec<u8>,
        description: String,
        parallelism: usize,
    ) -> Self {
        Self {
            node_id: id,
            description,
            operator_chain: OperatorChain {
                operators: vec![ChainedLogicalOperator {
                    operator_id,
                    operator_name: name,
                    operator_config: config,
                }],
                edges: vec![],
            },
            parallelism,
        }
    }
}

impl Display for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl Debug for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.operator_chain
                .operators
                .iter()
                .map(|op| op.operator_id.clone())
                .collect::<Vec<_>>()
                .join(" -> "),
            self.parallelism
        )
    }
}
