use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef, Result, TableReference, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use super::{NamedNode, StreamExtension};
use crate::multifield_partial_ord;
use crate::sql::types::{StreamSchema, TIMESTAMP_FIELD};

pub(crate) const DEBEZIUM_UNROLLING_EXTENSION_NAME: &str = "DebeziumUnrollingExtension";
pub(crate) const TO_DEBEZIUM_EXTENSION_NAME: &str = "ToDebeziumExtension";

/// Unrolls a Debezium-formatted (before/after/op) stream into individual rows
/// with an updating metadata column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DebeziumUnrollingExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub primary_keys: Vec<usize>,
    primary_key_names: Arc<Vec<String>>,
}

multifield_partial_ord!(
    DebeziumUnrollingExtension,
    input,
    primary_keys,
    primary_key_names
);

impl DebeziumUnrollingExtension {
    pub(crate) fn as_debezium_schema(
        input_schema: &DFSchemaRef,
        qualifier: Option<TableReference>,
    ) -> Result<DFSchemaRef> {
        let timestamp_field = if input_schema.has_column_with_unqualified_name(TIMESTAMP_FIELD) {
            Some(
                input_schema
                    .field_with_unqualified_name(TIMESTAMP_FIELD)?
                    .clone(),
            )
        } else {
            None
        };
        let struct_schema: Vec<_> = input_schema
            .fields()
            .iter()
            .filter(|field| field.name() != TIMESTAMP_FIELD)
            .cloned()
            .collect();

        let struct_type = DataType::Struct(struct_schema.into());

        let before = Arc::new(Field::new("before", struct_type.clone(), true));
        let after = Arc::new(Field::new("after", struct_type, true));
        let op = Arc::new(Field::new("op", DataType::Utf8, true));
        let mut fields = vec![before, after, op];

        if let Some(ts) = timestamp_field {
            fields.push(Arc::new(ts));
        }

        let schema = match qualifier {
            Some(q) => DFSchema::try_from_qualified_schema(q, &Schema::new(fields))?,
            None => DFSchema::try_from(Schema::new(fields))?,
        };
        Ok(Arc::new(schema))
    }

    pub fn try_new(input: LogicalPlan, primary_keys: Arc<Vec<String>>) -> Result<Self> {
        let input_schema = input.schema();

        let Some(before_index) = input_schema.index_of_column_by_name(None, "before") else {
            return plan_err!("DebeziumUnrollingExtension requires a before column");
        };
        let Some(after_index) = input_schema.index_of_column_by_name(None, "after") else {
            return plan_err!("DebeziumUnrollingExtension requires an after column");
        };
        let Some(op_index) = input_schema.index_of_column_by_name(None, "op") else {
            return plan_err!("DebeziumUnrollingExtension requires an op column");
        };

        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();
        if before_type != after_type {
            return plan_err!(
                "before and after columns must have the same type, not {} and {}",
                before_type,
                after_type
            );
        }

        let op_type = input_schema.field(op_index).data_type();
        if *op_type != DataType::Utf8 {
            return plan_err!("op column must be a string, not {}", op_type);
        }

        let DataType::Struct(fields) = before_type else {
            return plan_err!(
                "before and after columns must be structs, not {}",
                before_type
            );
        };

        let primary_key_idx = primary_keys
            .iter()
            .map(|pk| fields.find(pk).map(|(i, _)| i))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "primary key field not found in Debezium schema".to_string(),
                )
            })?;

        let qualifier = match (
            input_schema.qualified_field(before_index).0,
            input_schema.qualified_field(after_index).0,
        ) {
            (Some(bq), Some(aq)) => {
                if bq != aq {
                    return plan_err!("before and after columns must have the same alias");
                }
                Some(bq.clone())
            }
            (None, None) => None,
            _ => return plan_err!("before and after columns must both have an alias or neither"),
        };

        let mut out_fields = fields.to_vec();

        let Some(input_ts_index) = input_schema.index_of_column_by_name(None, TIMESTAMP_FIELD)
        else {
            return plan_err!("DebeziumUnrollingExtension requires a timestamp field");
        };
        out_fields.push(Arc::new(input_schema.field(input_ts_index).clone()));

        let arrow_schema = Schema::new(out_fields);
        let schema = match qualifier {
            Some(q) => DFSchema::try_from_qualified_schema(q, &arrow_schema)?,
            None => DFSchema::try_from(arrow_schema)?,
        };

        Ok(Self {
            input,
            schema: Arc::new(schema),
            primary_keys: primary_key_idx,
            primary_key_names: primary_keys,
        })
    }
}

impl UserDefinedLogicalNodeCore for DebeziumUnrollingExtension {
    fn name(&self) -> &str {
        DEBEZIUM_UNROLLING_EXTENSION_NAME
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
        write!(f, "DebeziumUnrollingExtension")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Self::try_new(inputs[0].clone(), self.primary_key_names.clone())
    }
}

impl StreamExtension for DebeziumUnrollingExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }

    fn transparent(&self) -> bool {
        true
    }
}

/// Wraps an input stream into Debezium format (before/after/op) for updating sinks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ToDebeziumExtension {
    pub(crate) input: Arc<LogicalPlan>,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(ToDebeziumExtension, input);

impl ToDebeziumExtension {
    pub(crate) fn try_new(input: LogicalPlan) -> Result<Self> {
        let schema = DebeziumUnrollingExtension::as_debezium_schema(input.schema(), None)
            .expect("should be able to create ToDebeziumExtension");
        Ok(Self {
            input: Arc::new(input),
            schema,
        })
    }
}

impl UserDefinedLogicalNodeCore for ToDebeziumExtension {
    fn name(&self) -> &str {
        TO_DEBEZIUM_EXTENSION_NAME
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
        write!(f, "ToDebeziumExtension")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Self::try_new(inputs[0].clone())
    }
}

impl StreamExtension for ToDebeziumExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        StreamSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }

    fn transparent(&self) -> bool {
        true
    }
}
