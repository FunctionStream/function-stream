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

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{Column, Result as DFResult, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{ColumnUnnestList, Expr, LogicalPlan, Projection, Unnest};

use crate::sql::types::{DFField, fields_with_qualifiers, schema_from_df_fields};

pub const UNNESTED_COL: &str = "__unnested";

/// Rewrites projections containing `unnest()` calls into proper Unnest logical plans.
pub struct UnnestRewriter {}

impl UnnestRewriter {
    fn split_unnest(expr: Expr) -> DFResult<(Expr, Option<Expr>)> {
        let mut captured: Option<Expr> = None;

        let expr = expr.transform_up(|e| {
            if let Expr::ScalarFunction(ScalarFunction { func: udf, args }) = &e
                && udf.name() == "unnest"
            {
                match args.len() {
                    1 => {
                        if captured.replace(args[0].clone()).is_some() {
                            return plan_err!(
                                "Multiple unnests in expression, which is not allowed"
                            );
                        }
                        return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                            UNNESTED_COL,
                        ))));
                    }
                    n => {
                        panic!("Unnest has wrong number of arguments (expected 1, found {n})");
                    }
                }
            }
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, captured))
    }
}

impl TreeNodeRewriter for UnnestRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(projection) = &node else {
            if node.expressions().iter().any(|e| {
                let e = Self::split_unnest(e.clone());
                e.is_err() || e.unwrap().1.is_some()
            }) {
                return plan_err!("unnest is only supported in SELECT statements");
            }
            return Ok(Transformed::no(node));
        };

        let mut unnest = None;
        let exprs = projection
            .expr
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                let (expr, opt) = Self::split_unnest(expr)?;
                let is_unnest = if let Some(e) = opt {
                    if let Some(prev) = unnest.replace((e, i))
                        && &prev != unnest.as_ref().unwrap()
                    {
                        return plan_err!(
                            "Projection contains multiple unnests, which is not currently supported"
                        );
                    }
                    true
                } else {
                    false
                };

                Ok((expr, is_unnest))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some((unnest_inner, unnest_idx)) = unnest {
            let produce_list = Arc::new(LogicalPlan::Projection(Projection::try_new(
                exprs
                    .iter()
                    .cloned()
                    .map(|(e, is_unnest)| {
                        if is_unnest {
                            unnest_inner.clone().alias(UNNESTED_COL)
                        } else {
                            e
                        }
                    })
                    .collect(),
                projection.input.clone(),
            )?));

            let unnest_fields = fields_with_qualifiers(produce_list.schema())
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i == unnest_idx {
                        let DataType::List(inner) = f.data_type() else {
                            return plan_err!(
                                "Argument '{}' to unnest is not a List",
                                f.qualified_name()
                            );
                        };
                        Ok(DFField::new_unqualified(
                            UNNESTED_COL,
                            inner.data_type().clone(),
                            inner.is_nullable(),
                        ))
                    } else {
                        Ok((*f).clone())
                    }
                })
                .collect::<DFResult<Vec<_>>>()?;

            let unnest_node = LogicalPlan::Unnest(Unnest {
                exec_columns: vec![
                    DFField::from(produce_list.schema().qualified_field(unnest_idx))
                        .qualified_column(),
                ],
                input: produce_list,
                list_type_columns: vec![(
                    unnest_idx,
                    ColumnUnnestList {
                        output_column: Column::new_unqualified(UNNESTED_COL),
                        depth: 1,
                    },
                )],
                struct_type_columns: vec![],
                dependency_indices: vec![],
                schema: Arc::new(schema_from_df_fields(&unnest_fields)?),
                options: Default::default(),
            });

            let output_node = LogicalPlan::Projection(Projection::try_new(
                exprs
                    .iter()
                    .enumerate()
                    .map(|(i, (expr, has_unnest))| {
                        if *has_unnest {
                            expr.clone()
                        } else {
                            Expr::Column(
                                DFField::from(unnest_node.schema().qualified_field(i))
                                    .qualified_column(),
                            )
                        }
                    })
                    .collect(),
                Arc::new(unnest_node),
            )?);

            Ok(Transformed::yes(output_node))
        } else {
            Ok(Transformed::no(LogicalPlan::Projection(projection.clone())))
        }
    }
}
