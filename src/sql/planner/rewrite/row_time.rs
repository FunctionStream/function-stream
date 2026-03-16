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

use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{Column, Result as DFResult};
use datafusion::logical_expr::Expr;

use crate::sql::types::TIMESTAMP_FIELD;

/// Rewrites `row_time()` scalar function calls to a column reference on `_timestamp`.
pub struct RowTimeRewriter {}

impl TreeNodeRewriter for RowTimeRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if let Expr::ScalarFunction(func) = &node
            && func.name() == "row_time"
        {
            let transformed = Expr::Column(Column {
                relation: None,
                name: TIMESTAMP_FIELD.to_string(),
                spans: Default::default(),
            })
            .alias("row_time()");
            return Ok(Transformed::yes(transformed));
        }
        Ok(Transformed::no(node))
    }
}
