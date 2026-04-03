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

use crate::sql::common::constants::sql_field;
use crate::sql::extensions::AsyncFunctionExecutionNode;
use crate::sql::extensions::remote_table::RemoteTableBoundaryNode;
use crate::sql::schema::StreamSchemaProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{Column, Result as DFResult, TableReference, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use std::sync::Arc;
use std::time::Duration;

type AsyncSplitResult = (String, AsyncOptions, Vec<Expr>);

#[derive(Debug, Clone, Copy)]
pub struct AsyncOptions {
    pub ordered: bool,
    pub max_concurrency: usize,
    pub timeout: Duration,
}

pub struct AsyncUdfRewriter<'a> {
    provider: &'a StreamSchemaProvider,
}

impl<'a> AsyncUdfRewriter<'a> {
    pub fn new(provider: &'a StreamSchemaProvider) -> Self {
        Self { provider }
    }

    fn split_async(
        expr: Expr,
        provider: &StreamSchemaProvider,
    ) -> DFResult<(Expr, Option<AsyncSplitResult>)> {
        let mut found: Option<(String, AsyncOptions, Vec<Expr>)> = None;
        let expr = expr.transform_up(|e| {
            if let Expr::ScalarFunction(ScalarFunction { func: udf, args }) = &e {
                if let Some(opts) = provider.get_async_udf_options(udf.name()) {
                    if found
                        .replace((udf.name().to_string(), opts, args.clone()))
                        .is_some()
                    {
                        return plan_err!(
                            "multiple async calls in the same expression, which is not allowed"
                        );
                    }
                    return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        sql_field::ASYNC_RESULT,
                    ))));
                }
            }
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, found))
    }
}

impl TreeNodeRewriter for AsyncUdfRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(mut projection) = node else {
            for e in node.expressions() {
                if let (_, Some((udf, _, _))) = Self::split_async(e.clone(), self.provider)? {
                    return plan_err!(
                        "async UDFs are only supported in projections, but {udf} was called in another context"
                    );
                }
            }
            return Ok(Transformed::no(node));
        };

        let mut args = None;
        for e in projection.expr.iter_mut() {
            let (new_e, Some(udf)) = Self::split_async(e.clone(), self.provider)? else {
                continue;
            };
            if let Some((prev, _, _)) = args.replace(udf) {
                return plan_err!(
                    "Projection contains multiple async UDFs, which is not supported \
                    \n(hint: two async UDF calls, {} and {}, appear in the same SELECT statement)",
                    prev,
                    args.unwrap().0
                );
            }
            *e = new_e;
        }

        let Some((name, opts, arg_exprs)) = args else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };
        let udf = self.provider.dylib_udfs.get(&name).unwrap().clone();

        let input = if matches!(*projection.input, LogicalPlan::Projection(..)) {
            Arc::new(LogicalPlan::Extension(Extension {
                node: Arc::new(RemoteTableBoundaryNode {
                    upstream_plan: (*projection.input).clone(),
                    table_identifier: TableReference::bare("subquery_projection"),
                    resolved_schema: projection.input.schema().clone(),
                    requires_materialization: false,
                }),
            }))
        } else {
            projection.input
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(AsyncFunctionExecutionNode {
                upstream_plan: input,
                operator_name: name,
                function_config: udf,
                invocation_args: arg_exprs,
                result_projections: projection.expr,
                preserve_ordering: opts.ordered,
                concurrency_limit: opts.max_concurrency,
                execution_timeout: opts.timeout,
                resolved_schema: projection.schema,
            }),
        })))
    }
}
