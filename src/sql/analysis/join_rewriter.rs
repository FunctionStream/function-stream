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

use crate::sql::schema::StreamSchemaProvider;
use crate::sql::extensions::join::StreamingJoinNode;
use crate::sql::extensions::key_calculation::KeyExtractionNode;
use crate::sql::analysis::streaming_window_analzer::StreamingWindowAnalzer;
use crate::sql::types::{WindowType, fields_with_qualifiers, schema_from_df_fields_with_metadata};
use crate::sql::common::constants::mem_exec_join_side;
use crate::sql::common::TIMESTAMP_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{
    Column, DataFusionError, JoinConstraint, JoinType, Result, ScalarValue, Spans, TableReference,
    not_impl_err, plan_err,
};
use datafusion::logical_expr::{
    self, BinaryExpr, Case, Expr, Extension, Join, LogicalPlan, Projection, build_join_schema,
};
use datafusion::prelude::coalesce;
use std::sync::Arc;

/// JoinRewriter handles the transformation of standard SQL joins into streaming-capable joins.
/// It manages stateful "Updating Joins" and time-aligned "Instant Joins".
pub(crate) struct JoinRewriter<'a> {
    pub schema_provider: &'a StreamSchemaProvider,
}

impl<'a> JoinRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    /// [Validation] Ensures left and right streams have compatible windowing strategies.
    fn validate_join_windows(&self, join: &Join) -> Result<bool> {
        let left_win = StreamingWindowAnalzer::get_window(&join.left)?;
        let right_win = StreamingWindowAnalzer::get_window(&join.right)?;

        match (left_win, right_win) {
            (None, None) => {
                if join.join_type == JoinType::Inner {
                    Ok(false) // Standard Updating Join (Inner)
                } else {
                    plan_err!(
                        "Non-inner joins (e.g., LEFT/RIGHT) require windowing to bound state."
                    )
                }
            }
            (Some(l), Some(r)) => {
                if l != r {
                    return plan_err!(
                        "Join window mismatch: left={:?}, right={:?}. Windows must match exactly.",
                        l,
                        r
                    );
                }
                if let WindowType::Session { .. } = l {
                    return plan_err!(
                        "Session windows are currently not supported in streaming joins."
                    );
                }
                Ok(true) // Instant Windowed Join
            }
            _ => plan_err!(
                "Mixed windowing detected. Both sides of a join must be either windowed or non-windowed."
            ),
        }
    }

    /// [Internal] Wraps a join input in a key-extraction layer to facilitate shuffle / key-by distribution.
    fn build_keyed_side(
        &self,
        input: Arc<LogicalPlan>,
        keys: Vec<Expr>,
        side: &str,
    ) -> Result<LogicalPlan> {
        let key_count = keys.len();

        let projection_exprs = keys
            .into_iter()
            .enumerate()
            .map(|(i, e)| {
                e.alias_qualified(Some(TableReference::bare("_stream")), format!("_key_{i}"))
            })
            .chain(
                fields_with_qualifiers(input.schema())
                    .iter()
                    .map(|f| Expr::Column(f.qualified_column())),
            )
            .collect();

        let projection = Projection::try_new(projection_exprs, input)?;
        let key_ext = KeyExtractionNode::try_new_with_projection(
            LogicalPlan::Projection(projection),
            (0..key_count).collect(),
            side.to_string(),
        )?;

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(key_ext),
        }))
    }

    /// [Strategy] Resolves the output timestamp of the join.
    /// Streaming joins must output the 'max' of the two input timestamps to ensure Watermark progression.
    fn apply_timestamp_resolution(&self, join_plan: LogicalPlan) -> Result<LogicalPlan> {
        let schema = join_plan.schema();
        let all_fields = fields_with_qualifiers(schema);

        let timestamp_fields: Vec<_> = all_fields
            .iter()
            .filter(|f| f.name() == "_timestamp")
            .cloned()
            .collect();

        if timestamp_fields.len() != 2 {
            return plan_err!(
                "Streaming join requires exactly two input timestamp fields to resolve output time."
            );
        }

        // Project all fields except the two raw timestamps
        let mut exprs: Vec<_> = all_fields
            .iter()
            .filter(|f| f.name() != "_timestamp")
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        // Calculate: GREATEST(left._timestamp, right._timestamp)
        let left_ts = Expr::Column(timestamp_fields[0].qualified_column());
        let right_ts = Expr::Column(timestamp_fields[1].qualified_column());

        let max_ts_expr = Expr::Case(Case {
            expr: Some(Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left_ts.clone()),
                op: logical_expr::Operator::GtEq,
                right: Box::new(right_ts.clone()),
            }))),
            when_then_expr: vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
                    Box::new(left_ts.clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
                    Box::new(right_ts.clone()),
                ),
            ],
            else_expr: Some(Box::new(coalesce(vec![left_ts, right_ts]))),
        })
        .alias(TIMESTAMP_FIELD);

        exprs.push(max_ts_expr);

        let out_fields: Vec<_> = all_fields
            .iter()
            .filter(|f| f.name() != "_timestamp")
            .cloned()
            .chain(std::iter::once(timestamp_fields[0].clone()))
            .collect();

        let out_schema = Arc::new(schema_from_df_fields_with_metadata(
            &out_fields,
            schema.metadata().clone(),
        )?);

        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            exprs,
            Arc::new(join_plan),
            out_schema,
        )?))
    }
}

impl TreeNodeRewriter for JoinRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Join(join) = node else {
            return Ok(Transformed::no(node));
        };

        // 1. Validate Streaming Context
        let is_instant = self.validate_join_windows(&join)?;
        if join.join_constraint != JoinConstraint::On {
            return not_impl_err!("Only 'ON' join constraints are supported in streaming SQL.");
        }
        if join.on.is_empty() && !is_instant {
            return plan_err!("Updating joins require at least one equality condition (Equijoin).");
        }

        // 2. Prepare Keyed Inputs for Shuffle
        let (left_on, right_on): (Vec<_>, Vec<_>) = join.on.clone().into_iter().unzip();
        let keyed_left = self.build_keyed_side(join.left, left_on, mem_exec_join_side::LEFT)?;
        let keyed_right = self.build_keyed_side(join.right, right_on, mem_exec_join_side::RIGHT)?;

        // 3. Assemble Rewritten Join Node
        let join_schema = Arc::new(build_join_schema(
            keyed_left.schema(),
            keyed_right.schema(),
            &join.join_type,
        )?);
        let rewritten_join = LogicalPlan::Join(Join {
            left: Arc::new(keyed_left),
            right: Arc::new(keyed_right),
            on: join.on,
            filter: join.filter,
            join_type: join.join_type,
            join_constraint: JoinConstraint::On,
            schema: join_schema,
            null_equals_null: false,
        });

        // 4. Resolve Output Watermark (Timestamp Projection)
        let plan_with_timestamp = self.apply_timestamp_resolution(rewritten_join)?;

        // 5. Wrap in StreamingJoinNode for physical planning
        let state_retention_ttl = (!is_instant).then_some(self.schema_provider.planning_options.ttl);
        let extension = StreamingJoinNode::new(
            plan_with_timestamp,
            is_instant,
            state_retention_ttl,
        );

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(extension),
        })))
    }
}
