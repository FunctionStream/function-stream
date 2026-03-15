use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchemaRef, Result, ScalarValue, internal_err};
use datafusion::logical_expr;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore, expr::ScalarFunction,
};

use crate::multifield_partial_ord;
use crate::sql::planner::extension::{NamedNode, StreamExtension, TimestampAppendExtension};
use crate::sql::planner::types::{
    DFField, StreamSchema, TIMESTAMP_FIELD, WindowBehavior, WindowType, fields_with_qualifiers,
    schema_from_df_fields, schema_from_df_fields_with_metadata,
};

pub(crate) const AGGREGATE_EXTENSION_NAME: &str = "AggregateExtension";

/// Extension node for windowed aggregate operations in streaming SQL.
/// Supports tumbling, sliding, session, and instant window aggregations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AggregateExtension {
    pub(crate) window_behavior: WindowBehavior,
    pub(crate) aggregate: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
}

multifield_partial_ord!(AggregateExtension, aggregate, key_fields, final_calculation);

impl AggregateExtension {
    pub fn new(
        window_behavior: WindowBehavior,
        aggregate: LogicalPlan,
        key_fields: Vec<usize>,
    ) -> Self {
        let final_calculation =
            Self::final_projection(&aggregate, window_behavior.clone()).unwrap();
        Self {
            window_behavior,
            aggregate,
            schema: final_calculation.schema().clone(),
            key_fields,
            final_calculation,
        }
    }

    /// Build the final projection after aggregation, which adds the window struct
    /// and computes the output timestamp based on the window behavior.
    pub fn final_projection(
        aggregate_plan: &LogicalPlan,
        window_behavior: WindowBehavior,
    ) -> Result<LogicalPlan> {
        let timestamp_field: DFField = aggregate_plan.inputs()[0]
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)?
            .into();
        let timestamp_append = LogicalPlan::Extension(Extension {
            node: Arc::new(TimestampAppendExtension::new(
                aggregate_plan.clone(),
                timestamp_field.qualifier().cloned(),
            )),
        });
        let mut aggregate_fields = fields_with_qualifiers(aggregate_plan.schema());
        let mut aggregate_expressions: Vec<_> = aggregate_fields
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect();

        let (window_field, window_index, width, is_nested) = match window_behavior {
            WindowBehavior::InData => return Ok(timestamp_append),
            WindowBehavior::FromOperator {
                window,
                window_field,
                window_index,
                is_nested,
            } => match window {
                WindowType::Tumbling { width, .. } | WindowType::Sliding { width, .. } => {
                    (window_field, window_index, width, is_nested)
                }
                WindowType::Session { .. } => {
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(WindowAppendExtension::new(
                            timestamp_append,
                            window_field,
                            window_index,
                        )),
                    }));
                }
                WindowType::Instant => return Ok(timestamp_append),
            },
        };

        if is_nested {
            return Self::nested_final_projection(
                timestamp_append,
                window_field,
                window_index,
                width,
            );
        }

        let timestamp_column =
            Column::new(timestamp_field.qualifier().cloned(), timestamp_field.name());
        aggregate_fields.insert(window_index, window_field.clone());

        let window_expression = Self::build_window_struct_expr(&timestamp_column, width);
        aggregate_expressions.insert(
            window_index,
            window_expression
                .alias_qualified(window_field.qualifier().cloned(), window_field.name()),
        );
        aggregate_fields.push(timestamp_field);

        let bin_end_calculation = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(timestamp_column.clone())),
            op: logical_expr::Operator::Plus,
            right: Box::new(Expr::Literal(
                ScalarValue::IntervalMonthDayNano(Some(
                    datafusion::arrow::datatypes::IntervalMonthDayNanoType::make_value(
                        0,
                        0,
                        (width.as_nanos() - 1) as i64,
                    ),
                )),
                None,
            )),
        });
        aggregate_expressions.push(bin_end_calculation);

        Ok(LogicalPlan::Projection(
            logical_expr::Projection::try_new_with_schema(
                aggregate_expressions,
                Arc::new(timestamp_append),
                Arc::new(schema_from_df_fields(&aggregate_fields)?),
            )?,
        ))
    }

    fn build_window_struct_expr(timestamp_column: &Column, width: Duration) -> Expr {
        let start_expr = Expr::Column(timestamp_column.clone());
        let end_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(timestamp_column.clone())),
            op: logical_expr::Operator::Plus,
            right: Box::new(Expr::Literal(
                ScalarValue::IntervalMonthDayNano(Some(
                    datafusion::arrow::datatypes::IntervalMonthDayNanoType::make_value(
                        0,
                        0,
                        width.as_nanos() as i64,
                    ),
                )),
                None,
            )),
        });

        Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                WindowStructUdf {},
            )),
            args: vec![start_expr, end_expr],
        })
    }

    fn nested_final_projection(
        aggregate_plan: LogicalPlan,
        window_field: DFField,
        window_index: usize,
        width: Duration,
    ) -> Result<LogicalPlan> {
        let timestamp_field: DFField = aggregate_plan
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
            .unwrap()
            .into();
        let timestamp_column =
            Column::new(timestamp_field.qualifier().cloned(), timestamp_field.name());

        let mut aggregate_fields = fields_with_qualifiers(aggregate_plan.schema());
        let mut aggregate_expressions: Vec<_> = aggregate_fields
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect();
        aggregate_fields.insert(window_index, window_field.clone());

        let window_expression = Self::build_window_struct_expr(&timestamp_column, width);
        aggregate_expressions.insert(
            window_index,
            window_expression
                .alias_qualified(window_field.qualifier().cloned(), window_field.name()),
        );

        Ok(LogicalPlan::Projection(
            logical_expr::Projection::try_new_with_schema(
                aggregate_expressions,
                Arc::new(aggregate_plan),
                Arc::new(schema_from_df_fields(&aggregate_fields).unwrap()),
            )
            .unwrap(),
        ))
    }
}

impl UserDefinedLogicalNodeCore for AggregateExtension {
    fn name(&self) -> &str {
        AGGREGATE_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.aggregate]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "AggregateExtension: {} | window_behavior: {:?}",
            self.schema(),
            match &self.window_behavior {
                WindowBehavior::InData => "InData".to_string(),
                WindowBehavior::FromOperator { window, .. } => format!("FromOperator({window:?})"),
            }
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }
        Ok(Self::new(
            self.window_behavior.clone(),
            inputs[0].clone(),
            self.key_fields.clone(),
        ))
    }
}

impl StreamExtension for AggregateExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        let output_schema = (*self.schema).clone().into();
        StreamSchema::from_schema_keys(Arc::new(output_schema), vec![]).unwrap()
    }
}

/// Extension for appending window struct (start, end) to the output
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct WindowAppendExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) window_field: DFField,
    pub(crate) window_index: usize,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(WindowAppendExtension, input, window_index);

impl WindowAppendExtension {
    fn new(input: LogicalPlan, window_field: DFField, window_index: usize) -> Self {
        let mut fields = fields_with_qualifiers(input.schema());
        fields.insert(window_index, window_field.clone());
        let metadata = input.schema().metadata().clone();
        Self {
            input,
            window_field,
            window_index,
            schema: Arc::new(schema_from_df_fields_with_metadata(&fields, metadata).unwrap()),
        }
    }
}

impl UserDefinedLogicalNodeCore for WindowAppendExtension {
    fn name(&self) -> &str {
        "WindowAppendExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WindowAppendExtension: field {:?} at {}",
            self.window_field, self.window_index
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self::new(
            inputs[0].clone(),
            self.window_field.clone(),
            self.window_index,
        ))
    }
}

/// Placeholder UDF to construct the window struct at plan time
#[derive(Debug)]
struct WindowStructUdf;

impl datafusion::logical_expr::ScalarUDFImpl for WindowStructUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "window"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &datafusion::logical_expr::Signature {
            type_signature: datafusion::logical_expr::TypeSignature::Any(2),
            volatility: datafusion::logical_expr::Volatility::Immutable,
        }
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(crate::sql::planner::schemas::window_arrow_struct())
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        unimplemented!("WindowStructUdf is a plan-time-only function")
    }
}
