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
use datafusion::common::{DFSchema, DataFusionError, Result, not_impl_err, plan_err};
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan, Projection};
use datafusion::prelude::col;
use std::sync::Arc;

use crate::sql::analysis::streaming_window_analzer::StreamingWindowAnalzer;
use crate::sql::logical_node::aggregate::StreamWindowAggregateNode;
use crate::sql::logical_node::key_calculation::{KeyExtractionNode, KeyExtractionStrategy};
use crate::sql::logical_node::updating_aggregate::ContinuousAggregateNode;
use crate::sql::schema::StreamSchemaProvider;
use crate::sql::types::{
    QualifiedField, TIMESTAMP_FIELD, WindowBehavior, WindowType, build_df_schema_with_metadata,
    extract_qualified_fields, extract_window_type,
};

/// AggregateRewriter transforms batch DataFusion aggregates into streaming stateful operators.
/// It handles windowing (Tumble/Hop/Session), watermarks, and continuous updating aggregates.
pub(crate) struct AggregateRewriter<'a> {
    pub schema_provider: &'a StreamSchemaProvider,
}

impl TreeNodeRewriter for AggregateRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Aggregate(mut agg) = node else {
            return Ok(Transformed::no(node));
        };

        // 1. Identify windowing functions (e.g., tumble, hop) in GROUP BY.
        let mut window_exprs: Vec<_> = agg
            .group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                extract_window_type(e)
                    .map(|opt| opt.map(|w| (i, w)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        if window_exprs.len() > 1 {
            return not_impl_err!("Streaming aggregates support at most one window expression");
        }

        // 2. Prepare internal metadata for Key-based distribution.
        let mut key_fields: Vec<QualifiedField> = extract_qualified_fields(&agg.schema)
            .iter()
            .take(agg.group_expr.len())
            .map(|f| {
                QualifiedField::new(
                    f.qualifier().cloned(),
                    format!("_key_{}", f.name()),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();

        // 3. Dispatch to ContinuousAggregateNode (UpdatingAggregate) if no windowing is detected.
        let input_window = StreamingWindowAnalzer::get_window(&agg.input)?;
        if window_exprs.is_empty() && input_window.is_none() {
            return self.rewrite_as_continuous_updating_aggregate(
                agg.input,
                key_fields,
                agg.group_expr,
                agg.aggr_expr,
                agg.schema,
            );
        }

        // 4. Resolve Windowing Strategy (InData vs FromOperator).
        let behavior = self.resolve_window_context(
            &agg.input,
            &mut agg.group_expr,
            &agg.schema,
            &mut window_exprs,
        )?;

        // Adjust keys if windowing is handled by the operator.
        if let WindowBehavior::FromOperator { window_index, .. } = &behavior {
            key_fields.remove(*window_index);
        }

        let key_count = key_fields.len();
        let keyed_input =
            self.build_keyed_input(agg.input.clone(), &agg.group_expr, &key_fields)?;

        // 5. Build the final StreamWindowAggregateNode for the physical planner.
        let mut internal_fields = extract_qualified_fields(&agg.schema);
        if let WindowBehavior::FromOperator { window_index, .. } = &behavior {
            internal_fields.remove(*window_index);
        }
        let internal_schema = Arc::new(build_df_schema_with_metadata(
            &internal_fields,
            agg.schema.metadata().clone(),
        )?);

        let rewritten_agg = Aggregate::try_new_with_schema(
            Arc::new(keyed_input),
            agg.group_expr,
            agg.aggr_expr,
            internal_schema,
        )?;

        let extension = StreamWindowAggregateNode::try_new(
            behavior,
            LogicalPlan::Aggregate(rewritten_agg),
            (0..key_count).collect(),
        )?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(extension),
        })))
    }
}

impl<'a> AggregateRewriter<'a> {
    pub fn new(schema_provider: &'a StreamSchemaProvider) -> Self {
        Self { schema_provider }
    }

    /// [Internal] Builds the physical Key Calculation layer required for distributed Shuffling.
    /// This wraps the input in a Projection and a KeyExtractionNode.
    fn build_keyed_input(
        &self,
        input: Arc<LogicalPlan>,
        group_expr: &[Expr],
        key_fields: &[QualifiedField],
    ) -> Result<LogicalPlan> {
        let key_count = group_expr.len();
        let mut projection_fields = key_fields.to_vec();
        projection_fields.extend(extract_qualified_fields(input.schema()));

        let key_schema = Arc::new(build_df_schema_with_metadata(
            &projection_fields,
            input.schema().metadata().clone(),
        )?);

        // Map group expressions to '_key_' aliases while passing through all original columns.
        let mut exprs: Vec<_> = group_expr
            .iter()
            .zip(key_fields.iter())
            .map(|(expr, f)| expr.clone().alias(f.name().to_string()))
            .collect();

        exprs.extend(
            extract_qualified_fields(input.schema())
                .iter()
                .map(|f| Expr::Column(f.qualified_column())),
        );

        let projection =
            LogicalPlan::Projection(Projection::try_new_with_schema(exprs, input, key_schema)?);

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(KeyExtractionNode::new(
                projection,
                KeyExtractionStrategy::ColumnIndices((0..key_count).collect()),
            )),
        }))
    }

    /// [Strategy] Rewrites standard GROUP BY into a ContinuousAggregateNode with retraction semantics.
    /// Injected max(_timestamp) ensures the streaming pulse (Watermark) continues to propagate.
    fn rewrite_as_continuous_updating_aggregate(
        &self,
        input: Arc<LogicalPlan>,
        key_fields: Vec<QualifiedField>,
        group_expr: Vec<Expr>,
        mut aggr_expr: Vec<Expr>,
        schema: Arc<DFSchema>,
    ) -> Result<Transformed<LogicalPlan>> {
        let key_count = key_fields.len();
        let keyed_input = self.build_keyed_input(input, &group_expr, &key_fields)?;

        // Ensure the updating stream maintains time awareness.
        let timestamp_col = keyed_input
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
            .map_err(|_| {
                DataFusionError::Plan(
                    "Required _timestamp field missing for updating aggregate".to_string(),
                )
            })?;

        let timestamp_field: QualifiedField = timestamp_col.into();
        aggr_expr.push(max(col(timestamp_field.qualified_column())).alias(TIMESTAMP_FIELD));

        let mut output_fields = extract_qualified_fields(&schema);
        output_fields.push(timestamp_field);

        let output_schema = Arc::new(build_df_schema_with_metadata(
            &output_fields,
            schema.metadata().clone(),
        )?);

        let base_aggregate = Aggregate::try_new_with_schema(
            Arc::new(keyed_input),
            group_expr,
            aggr_expr,
            output_schema,
        )?;

        let continuous_node = ContinuousAggregateNode::try_new(
            LogicalPlan::Aggregate(base_aggregate),
            (0..key_count).collect(),
            None,
            self.schema_provider.planning_options.ttl,
        )?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(continuous_node),
        })))
    }

    /// [Strategy] Reconciles window definitions between the input stream and the current GROUP BY.
    fn resolve_window_context(
        &self,
        input: &LogicalPlan,
        group_expr: &mut Vec<Expr>,
        schema: &DFSchema,
        window_expr_info: &mut Vec<(usize, WindowType)>,
    ) -> Result<WindowBehavior> {
        let mut visitor = StreamingWindowAnalzer::default();
        input.visit_with_subqueries(&mut visitor)?;

        let input_window = visitor.window;
        let has_group_window = !window_expr_info.is_empty();

        match (input_window, has_group_window) {
            (Some(i_win), true) => {
                let (idx, g_win) = window_expr_info.pop().unwrap();
                if i_win != g_win {
                    return plan_err!("Inconsistent windowing detected");
                }

                if let Some(field) = visitor.fields.iter().next() {
                    group_expr[idx] = Expr::Column(field.qualified_column());
                    Ok(WindowBehavior::InData)
                } else {
                    group_expr.remove(idx);
                    Ok(WindowBehavior::FromOperator {
                        window: i_win,
                        window_field: schema.qualified_field(idx).into(),
                        window_index: idx,
                        is_nested: true,
                    })
                }
            }
            (None, true) => {
                let (idx, g_win) = window_expr_info.pop().unwrap();
                group_expr.remove(idx);
                Ok(WindowBehavior::FromOperator {
                    window: g_win,
                    window_field: schema.qualified_field(idx).into(),
                    window_index: idx,
                    is_nested: false,
                })
            }
            (Some(_), false) => Ok(WindowBehavior::InData),
            _ => unreachable!("Handled by updating path"),
        }
    }
}
