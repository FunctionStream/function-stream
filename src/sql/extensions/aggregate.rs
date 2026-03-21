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

use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::types::IntervalMonthDayNanoType;
use datafusion::common::{Column, DFSchemaRef, Result, ScalarValue, internal_err};
use datafusion::logical_expr::{
    self, expr::ScalarFunction, BinaryExpr, Expr, Extension, LogicalPlan,
    UserDefinedLogicalNodeCore,
};
use datafusion_common::{plan_err, DFSchema, DataFusionError};
use datafusion_expr::Aggregate;
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use protocol::grpc::api::{
    SessionWindowAggregateOperator, SlidingWindowAggregateOperator, TumblingWindowAggregateOperator,
};

use crate::multifield_partial_ord;
use crate::sql::common::{FsSchema, FsSchemaRef};
use crate::sql::extensions::{
    CompiledTopologyNode, StreamingOperatorBlueprint, SystemTimestampInjectorNode,
};
use crate::sql::logical_node::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use crate::sql::logical_planner::planner::{NamedNode, Planner, SplitPlanOutput};
use crate::sql::logical_planner::{window, FsPhysicalExtensionCodec};
use crate::sql::types::{
    DFField, TIMESTAMP_FIELD, WindowBehavior, WindowType, fields_with_qualifiers,
    schema_from_df_fields, schema_from_df_fields_with_metadata,
};

pub(crate) const STREAM_AGG_EXTENSION_NAME: &str = "StreamWindowAggregateNode";
const INTERNAL_TIMESTAMP_COL: &str = "_timestamp";

/// Represents a streaming windowed aggregation node in the logical plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamWindowAggregateNode {
    pub(crate) window_spec: WindowBehavior,
    pub(crate) base_agg_plan: LogicalPlan,
    pub(crate) output_schema: DFSchemaRef,
    pub(crate) partition_keys: Vec<usize>,
    pub(crate) post_aggregation_plan: LogicalPlan,
}

multifield_partial_ord!(
    StreamWindowAggregateNode,
    base_agg_plan,
    partition_keys,
    post_aggregation_plan
);

impl StreamWindowAggregateNode {
    /// Safely constructs a new node, computing the final projection without panicking.
    pub fn try_new(
        window_spec: WindowBehavior,
        base_agg_plan: LogicalPlan,
        partition_keys: Vec<usize>,
    ) -> Result<Self> {
        let post_aggregation_plan =
            WindowBoundaryMath::build_post_aggregation(&base_agg_plan, window_spec.clone())?;

        Ok(Self {
            window_spec,
            base_agg_plan,
            output_schema: post_aggregation_plan.schema().clone(),
            partition_keys,
            post_aggregation_plan,
        })
    }

    fn build_tumbling_operator(
        &self,
        planner: &Planner,
        node_id: usize,
        input_schema: DFSchemaRef,
        duration: Duration,
    ) -> Result<LogicalNode> {
        let binning_expr = planner.binning_function_proto(duration, input_schema.clone())?;

        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = planner.split_physical_plan(self.partition_keys.clone(), &self.base_agg_plan, true)?;

        let final_physical = planner.sync_plan(&self.post_aggregation_plan)?;
        let final_physical_proto = PhysicalPlanNode::try_from_physical_plan(
            final_physical,
            &FsPhysicalExtensionCodec::default(),
        )?;

        let operator_config = TumblingWindowAggregateOperator {
            name: "TumblingWindow".to_string(),
            width_micros: duration.as_micros() as u64,
            binning_function: binning_expr.encode_to_vec(),
            input_schema: Some(
                FsSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    self.partition_keys.clone(),
                )?
                .into(),
            ),
            partial_schema: Some(partial_schema.into()),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: Some(final_physical_proto.encode_to_vec()),
        };

        Ok(LogicalNode::single(
            node_id as u32,
            format!("tumbling_{node_id}"),
            OperatorName::TumblingWindowAggregate,
            operator_config.encode_to_vec(),
            format!("TumblingWindow<{}>", operator_config.name),
            1,
        ))
    }

    fn build_sliding_operator(
        &self,
        planner: &Planner,
        node_id: usize,
        input_schema: DFSchemaRef,
        duration: Duration,
        slide_interval: Duration,
    ) -> Result<LogicalNode> {
        let binning_expr = planner.binning_function_proto(slide_interval, input_schema.clone())?;

        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = planner.split_physical_plan(self.partition_keys.clone(), &self.base_agg_plan, true)?;

        let final_physical = planner.sync_plan(&self.post_aggregation_plan)?;
        let final_physical_proto = PhysicalPlanNode::try_from_physical_plan(
            final_physical,
            &FsPhysicalExtensionCodec::default(),
        )?;

        let operator_config = SlidingWindowAggregateOperator {
            name: format!("SlidingWindow<{duration:?}>"),
            width_micros: duration.as_micros() as u64,
            slide_micros: slide_interval.as_micros() as u64,
            binning_function: binning_expr.encode_to_vec(),
            input_schema: Some(
                FsSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    self.partition_keys.clone(),
                )?
                .into(),
            ),
            partial_schema: Some(partial_schema.into()),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: final_physical_proto.encode_to_vec(),
        };

        Ok(LogicalNode::single(
            node_id as u32,
            format!("sliding_window_{node_id}"),
            OperatorName::SlidingWindowAggregate,
            operator_config.encode_to_vec(),
            "sliding window".to_string(),
            1,
        ))
    }

    fn build_session_operator(
        &self,
        planner: &Planner,
        node_id: usize,
        input_schema: DFSchemaRef,
    ) -> Result<LogicalNode> {
        let WindowBehavior::FromOperator {
            window: WindowType::Session { gap },
            window_index,
            window_field,
            is_nested: false,
        } = &self.window_spec
        else {
            return plan_err!("Expected standard session window configuration");
        };

        let output_fields = fields_with_qualifiers(self.base_agg_plan.schema());
        let LogicalPlan::Aggregate(base_agg) = self.base_agg_plan.clone() else {
            return plan_err!("Base plan must be an Aggregate node");
        };

        let key_count = self.partition_keys.len();
        let unkeyed_schema = Arc::new(schema_from_df_fields_with_metadata(
            &output_fields[key_count..],
            self.base_agg_plan.schema().metadata().clone(),
        )?);

        let unkeyed_agg_node = Aggregate::try_new_with_schema(
            base_agg.input.clone(),
            vec![],
            base_agg.aggr_expr.clone(),
            unkeyed_schema,
        )?;

        let physical_agg = planner.sync_plan(&LogicalPlan::Aggregate(unkeyed_agg_node))?;
        let physical_agg_proto = PhysicalPlanNode::try_from_physical_plan(
            physical_agg,
            &FsPhysicalExtensionCodec::default(),
        )?;

        let operator_config = SessionWindowAggregateOperator {
            name: format!("session_window_{node_id}"),
            gap_micros: gap.as_micros() as u64,
            window_field_name: window_field.name().to_string(),
            window_index: *window_index as u64,
            input_schema: Some(
                FsSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    self.partition_keys.clone(),
                )?
                .into(),
            ),
            unkeyed_aggregate_schema: None,
            partial_aggregation_plan: vec![],
            final_aggregation_plan: physical_agg_proto.encode_to_vec(),
        };

        Ok(LogicalNode::single(
            node_id as u32,
            format!("SessionWindow<{gap:?}>"),
            OperatorName::SessionWindowAggregate,
            operator_config.encode_to_vec(),
            operator_config.name.clone(),
            1,
        ))
    }

    fn build_instant_operator(
        &self,
        planner: &Planner,
        node_id: usize,
        input_schema: DFSchemaRef,
        apply_final_projection: bool,
    ) -> Result<LogicalNode> {
        let ts_column_expr =
            Expr::Column(Column::new_unqualified(INTERNAL_TIMESTAMP_COL.to_string()));
        let binning_expr = planner.create_physical_expr(&ts_column_expr, &input_schema)?;
        let binning_proto = serialize_physical_expr(&binning_expr, &DefaultPhysicalExtensionCodec {})?;

        let final_projection_payload = if apply_final_projection {
            let physical_plan = planner.sync_plan(&self.post_aggregation_plan)?;
            let proto_node = PhysicalPlanNode::try_from_physical_plan(
                physical_plan,
                &FsPhysicalExtensionCodec::default(),
            )?;
            Some(proto_node.encode_to_vec())
        } else {
            None
        };

        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = planner.split_physical_plan(self.partition_keys.clone(), &self.base_agg_plan, true)?;

        let operator_config = TumblingWindowAggregateOperator {
            name: "InstantWindow".to_string(),
            width_micros: 0,
            binning_function: binning_proto.encode_to_vec(),
            input_schema: Some(
                FsSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    self.partition_keys.clone(),
                )?
                .into(),
            ),
            partial_schema: Some(partial_schema.into()),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: final_projection_payload,
        };

        Ok(LogicalNode::single(
            node_id as u32,
            format!("instant_window_{node_id}"),
            OperatorName::TumblingWindowAggregate,
            operator_config.encode_to_vec(),
            "instant window".to_string(),
            1,
        ))
    }
}

impl StreamingOperatorBlueprint for StreamWindowAggregateNode {
    fn operator_identity(&self) -> Option<NamedNode> {
        None
    }

    fn compile_to_graph_node(
        &self,
        planner: &Planner,
        node_id: usize,
        mut input_schemas: Vec<FsSchemaRef>,
    ) -> Result<CompiledTopologyNode> {
        if input_schemas.len() != 1 {
            return plan_err!("StreamWindowAggregateNode requires exactly one input schema");
        }

        let raw_schema = input_schemas.remove(0);
        let df_schema = Arc::new(DFSchema::try_from(raw_schema.schema.as_ref().clone())?);

        let logical_operator = match &self.window_spec {
            WindowBehavior::FromOperator { window, is_nested, .. } => {
                if *is_nested {
                    self.build_instant_operator(planner, node_id, df_schema, true)?
                } else {
                    match window {
                        WindowType::Tumbling { width } => {
                            self.build_tumbling_operator(planner, node_id, df_schema, *width)?
                        }
                        WindowType::Sliding { width, slide } => {
                            self.build_sliding_operator(planner, node_id, df_schema, *width, *slide)?
                        }
                        WindowType::Session { .. } => {
                            self.build_session_operator(planner, node_id, df_schema)?
                        }
                        WindowType::Instant => {
                            return plan_err!(
                                "Instant window is invalid within standard operator context"
                            );
                        }
                    }
                }
            }
            WindowBehavior::InData => self
                .build_instant_operator(planner, node_id, df_schema, false)
                .map_err(|e| e.context("Failed compiling instant window"))?,
        };

        let link = LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*raw_schema).clone());
        Ok(CompiledTopologyNode {
            execution_unit: logical_operator,
            routing_edges: vec![link],
        })
    }

    fn yielded_schema(&self) -> FsSchema {
        let schema_ref = (*self.output_schema).clone().into();
        FsSchema::from_schema_unkeyed(Arc::new(schema_ref)).expect(
            "StreamWindowAggregateNode output schema must contain timestamp column",
        )
    }
}

impl UserDefinedLogicalNodeCore for StreamWindowAggregateNode {
    fn name(&self) -> &str {
        STREAM_AGG_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.base_agg_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        let spec_desc = match &self.window_spec {
            WindowBehavior::InData => "InData".to_string(),
            WindowBehavior::FromOperator { window, .. } => format!("FromOperator({window:?})"),
        };
        write!(
            f,
            "StreamWindowAggregate: {} | spec: {}",
            self.schema(),
            spec_desc
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("StreamWindowAggregateNode expects exactly 1 input");
        }
        Self::try_new(
            self.window_spec.clone(),
            inputs[0].clone(),
            self.partition_keys.clone(),
        )
    }
}

// -----------------------------------------------------------------------------
// Dedicated boundary math for window bin / post-aggregation projection
// -----------------------------------------------------------------------------

struct WindowBoundaryMath;

impl WindowBoundaryMath {
    fn interval_nanos(nanos: i64) -> Expr {
        Expr::Literal(
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(0, 0, nanos),
            )),
            None,
        )
    }

    fn build_post_aggregation(
        agg_plan: &LogicalPlan,
        window_spec: WindowBehavior,
    ) -> Result<LogicalPlan> {
        let ts_field: DFField = agg_plan
            .inputs()
            .first()
            .ok_or_else(|| DataFusionError::Plan("Aggregate has no inputs".into()))?
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)?
            .into();

        let plan_with_ts = LogicalPlan::Extension(Extension {
            node: Arc::new(SystemTimestampInjectorNode::try_new(
                agg_plan.clone(),
                ts_field.qualifier().cloned(),
            )?),
        });

        let (win_field, win_index, duration, is_nested) = match window_spec {
            WindowBehavior::InData => return Ok(plan_with_ts),
            WindowBehavior::FromOperator {
                window,
                window_field,
                window_index,
                is_nested,
            } => match window {
                WindowType::Tumbling { width } | WindowType::Sliding { width, .. } => {
                    (window_field, window_index, width, is_nested)
                }
                WindowType::Session { .. } => {
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(InjectWindowFieldNode::try_new(
                            plan_with_ts,
                            window_field,
                            window_index,
                        )?),
                    }));
                }
                WindowType::Instant => return Ok(plan_with_ts),
            },
        };

        if is_nested {
            return Self::build_nested_projection(plan_with_ts, win_field, win_index, duration);
        }

        let mut output_fields = fields_with_qualifiers(agg_plan.schema());
        let mut projections: Vec<_> = output_fields
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        let ts_col_expr = Expr::Column(Column::new(ts_field.qualifier().cloned(), ts_field.name()));

        output_fields.insert(win_index, win_field.clone());

        let win_func_expr = Expr::ScalarFunction(ScalarFunction {
            func: window(),
            args: vec![
                ts_col_expr.clone(),
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(ts_col_expr.clone()),
                    op: logical_expr::Operator::Plus,
                    right: Box::new(Self::interval_nanos(duration.as_nanos() as i64)),
                }),
            ],
        });

        projections.insert(
            win_index,
            win_func_expr.alias_qualified(win_field.qualifier().cloned(), win_field.name()),
        );

        output_fields.push(ts_field);

        let bin_end_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(ts_col_expr),
            op: logical_expr::Operator::Plus,
            right: Box::new(Self::interval_nanos((duration.as_nanos() - 1) as i64)),
        });
        projections.push(bin_end_expr);

        Ok(LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
            projections,
            Arc::new(plan_with_ts),
            Arc::new(schema_from_df_fields(&output_fields)?),
        )?))
    }

    fn build_nested_projection(
        plan: LogicalPlan,
        win_field: DFField,
        win_index: usize,
        duration: Duration,
    ) -> Result<LogicalPlan> {
        let ts_field: DFField = plan
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)?
            .into();
        let ts_col_expr = Expr::Column(Column::new(ts_field.qualifier().cloned(), ts_field.name()));

        let mut output_fields = fields_with_qualifiers(plan.schema());
        let mut projections: Vec<_> = output_fields
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        output_fields.insert(win_index, win_field.clone());

        let win_func_expr = Expr::ScalarFunction(ScalarFunction {
            func: window(),
            args: vec![
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(ts_col_expr.clone()),
                    op: logical_expr::Operator::Minus,
                    right: Box::new(Self::interval_nanos(duration.as_nanos() as i64 - 1)),
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(ts_col_expr),
                    op: logical_expr::Operator::Plus,
                    right: Box::new(Self::interval_nanos(1)),
                }),
            ],
        });

        projections.insert(
            win_index,
            win_func_expr.alias_qualified(win_field.qualifier().cloned(), win_field.name()),
        );

        Ok(LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
            projections,
            Arc::new(plan),
            Arc::new(schema_from_df_fields(&output_fields)?),
        )?))
    }
}

// -----------------------------------------------------------------------------
// Field injection node (session window column placement)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InjectWindowFieldNode {
    pub(crate) upstream_plan: LogicalPlan,
    pub(crate) target_field: DFField,
    pub(crate) insertion_index: usize,
    pub(crate) new_schema: DFSchemaRef,
}

multifield_partial_ord!(InjectWindowFieldNode, upstream_plan, insertion_index);

impl InjectWindowFieldNode {
    fn try_new(
        upstream_plan: LogicalPlan,
        target_field: DFField,
        insertion_index: usize,
    ) -> Result<Self> {
        let mut fields = fields_with_qualifiers(upstream_plan.schema());
        fields.insert(insertion_index, target_field.clone());
        let meta = upstream_plan.schema().metadata().clone();

        Ok(Self {
            upstream_plan,
            target_field,
            insertion_index,
            new_schema: Arc::new(schema_from_df_fields_with_metadata(&fields, meta)?),
        })
    }
}

impl UserDefinedLogicalNodeCore for InjectWindowFieldNode {
    fn name(&self) -> &str {
        "InjectWindowFieldNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.upstream_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.new_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "InjectWindowField: insert {:?} at offset {}",
            self.target_field, self.insertion_index
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("InjectWindowFieldNode expects exactly 1 input");
        }
        Self::try_new(
            inputs[0].clone(),
            self.target_field.clone(),
            self.insertion_index,
        )
    }
}
