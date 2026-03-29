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

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result as DFResult, plan_err, tree_node::TreeNodeRewriter};
use datafusion::logical_expr::{
    self, Expr, Extension, LogicalPlan, Projection, Sort, Window, expr::WindowFunction,
    expr::WindowFunctionParams,
};
use datafusion_common::DataFusionError;
use std::sync::Arc;
use tracing::debug;

use crate::sql::extensions::key_calculation::{KeyExtractionNode, KeyExtractionStrategy};
use crate::sql::extensions::windows_function::StreamingWindowFunctionNode;
use crate::sql::analysis::streaming_window_analzer::{StreamingWindowAnalzer, extract_column};
use crate::sql::types::{WindowType, fields_with_qualifiers, schema_from_df_fields};

/// WindowFunctionRewriter transforms standard SQL Window functions into streaming-compatible
/// stateful operators, ensuring proper data partitioning and sorting for distributed execution.
pub(crate) struct WindowFunctionRewriter;

impl WindowFunctionRewriter {
    /// Recursively unwraps Aliases to find the underlying WindowFunction.
    fn resolve_window_function(&self, expr: &Expr) -> DFResult<(WindowFunction, String)> {
        match expr {
            Expr::Alias(alias) => {
                let (func, _) = self.resolve_window_function(&alias.expr)?;
                Ok((func, alias.name.clone()))
            }
            Expr::WindowFunction(wf) => Ok((wf.as_ref().clone(), expr.name_for_alias()?)),
            _ => plan_err!("Expected WindowFunction or Alias, found: {:?}", expr),
        }
    }

    /// Identifies which field in the PARTITION BY clause corresponds to the streaming window.
    fn identify_window_partition(
        &self,
        params: &WindowFunctionParams,
        input: &LogicalPlan,
        input_window_fields: &std::collections::HashSet<crate::sql::types::DFField>,
    ) -> DFResult<usize> {
        let matched: Vec<_> = params
            .partition_by
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                let col = extract_column(e)?;
                let field = input
                    .schema()
                    .field_with_name(col.relation.as_ref(), &col.name)
                    .ok()?;
                let df_field = (col.relation.clone(), Arc::new(field.clone())).into();

                if input_window_fields.contains(&df_field) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        if matched.len() != 1 {
            return plan_err!(
                "Streaming window functions require exactly one window column in PARTITION BY. Found: {}",
                matched.len()
            );
        }
        Ok(matched[0])
    }

    /// Wraps the input in a Projection and KeyExtractionNode to handle data distribution.
    fn build_keyed_input(
        &self,
        input: Arc<LogicalPlan>,
        partition_keys: &[Expr],
    ) -> DFResult<LogicalPlan> {
        let key_count = partition_keys.len();

        // 1. Build projection: [_key_0, _key_1, ..., original_columns]
        let mut exprs: Vec<_> = partition_keys
            .iter()
            .enumerate()
            .map(|(i, e)| e.clone().alias(format!("_key_{i}")))
            .collect();

        exprs.extend(
            fields_with_qualifiers(input.schema())
                .iter()
                .map(|f| Expr::Column(f.qualified_column())),
        );

        // 2. Derive the keyed schema
        let mut keyed_fields =
            fields_with_qualifiers(&Projection::try_new(exprs.clone(), input.clone())?.schema)
                .iter()
                .take(key_count)
                .cloned()
                .collect::<Vec<_>>();
        keyed_fields.extend(fields_with_qualifiers(input.schema()));

        let keyed_schema = Arc::new(schema_from_df_fields(&keyed_fields)?);

        let projection =
            LogicalPlan::Projection(Projection::try_new_with_schema(exprs, input, keyed_schema)?);

        // 3. Wrap in KeyExtractionNode for the physical planner
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(KeyExtractionNode::new(
                projection,
                KeyExtractionStrategy::ColumnIndices((0..key_count).collect()),
            )),
        }))
    }
}

impl TreeNodeRewriter for WindowFunctionRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Window(window) = node else {
            return Ok(Transformed::no(node));
        };

        debug!("Rewriting window function for streaming: {:?}", window);

        // 1. Analyze input windowing context
        let mut analyzer = StreamingWindowAnalzer::default();
        window.input.visit_with_subqueries(&mut analyzer)?;

        let input_window = analyzer.window.ok_or_else(|| {
            DataFusionError::Plan(
                "Window functions require a windowed input stream (e.g., TUMBLE/HOP)".into(),
            )
        })?;

        if matches!(input_window, WindowType::Session { .. }) {
            return plan_err!(
                "Streaming window functions (OVER) are not supported on Session windows."
            );
        }

        // 2. Validate window expression constraints
        if window.window_expr.len() != 1 {
            return plan_err!(
                "Arroyo currently supports exactly one window expression per OVER clause."
            );
        }

        let (mut wf, original_name) = self.resolve_window_function(&window.window_expr[0])?;

        // 3. Identify and extract the window column from PARTITION BY
        let window_part_idx =
            self.identify_window_partition(&wf.params, &window.input, &analyzer.fields)?;
        let mut partition_keys = wf.params.partition_by.clone();
        partition_keys.remove(window_part_idx);

        // Update function params to exclude the window column from internal partitioning
        // as the streaming engine handles window boundaries natively.
        wf.params.partition_by = partition_keys.clone();
        let key_count = partition_keys.len();

        // 4. Build the data-shuffling pipeline (Projection -> KeyCalc -> Sort)
        let keyed_plan = self.build_keyed_input(window.input.clone(), &partition_keys)?;

        let mut sort_exprs: Vec<_> = partition_keys
            .iter()
            .map(|e| logical_expr::expr::Sort {
                expr: e.clone(),
                asc: true,
                nulls_first: false,
            })
            .collect();
        sort_exprs.extend(wf.params.order_by.clone());

        let sorted_plan = LogicalPlan::Sort(Sort {
            expr: sort_exprs,
            input: Arc::new(keyed_plan),
            fetch: None,
        });

        // 5. Final Assembly
        let final_wf_expr = Expr::WindowFunction(Box::new(wf)).alias_if_changed(original_name)?;
        let rewritten_window =
            LogicalPlan::Window(Window::try_new(vec![final_wf_expr], Arc::new(sorted_plan))?);

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(StreamingWindowFunctionNode::new(
                rewritten_window,
                (0..key_count).collect(),
            )),
        })))
    }
}
