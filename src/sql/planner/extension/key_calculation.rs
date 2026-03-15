use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchemaRef, Result, internal_err};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::multifield_partial_ord;
use crate::sql::planner::extension::{NamedNode, StreamExtension};
use crate::sql::planner::types::{
    StreamSchema, fields_with_qualifiers, schema_from_df_fields_with_metadata,
};

pub(crate) const KEY_CALCULATION_NAME: &str = "KeyCalculationExtension";

/// Two ways of specifying keys: column indices or expressions to evaluate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum KeysOrExprs {
    Keys(Vec<usize>),
    Exprs(Vec<Expr>),
}

/// Calculation for computing keyed data, used for shuffling data to correct nodes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeyCalculationExtension {
    pub(crate) name: Option<String>,
    pub(crate) input: LogicalPlan,
    pub(crate) keys: KeysOrExprs,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(KeyCalculationExtension, name, input, keys);

impl KeyCalculationExtension {
    pub fn new_named_and_trimmed(input: LogicalPlan, keys: Vec<usize>, name: String) -> Self {
        let output_fields: Vec<_> = fields_with_qualifiers(input.schema())
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if !keys.contains(&index) {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect();

        let schema =
            schema_from_df_fields_with_metadata(&output_fields, input.schema().metadata().clone())
                .unwrap();
        Self {
            name: Some(name),
            input,
            keys: KeysOrExprs::Keys(keys),
            schema: Arc::new(schema),
        }
    }

    pub fn new(input: LogicalPlan, keys: KeysOrExprs) -> Self {
        let schema = input.schema().clone();
        Self {
            name: None,
            input,
            keys,
            schema,
        }
    }
}

impl StreamExtension for KeyCalculationExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn output_schema(&self) -> StreamSchema {
        let input_schema = self.input.schema().as_ref();
        match &self.keys {
            KeysOrExprs::Keys(keys) => {
                StreamSchema::from_schema_keys(Arc::new(input_schema.into()), keys.clone()).unwrap()
            }
            KeysOrExprs::Exprs(exprs) => {
                let mut fields = vec![];
                for (i, e) in exprs.iter().enumerate() {
                    let (dt, nullable) = e.data_type_and_nullable(input_schema).unwrap();
                    fields.push(Field::new(format!("__key_{i}"), dt, nullable).into());
                }
                for f in input_schema.fields().iter() {
                    fields.push(f.clone());
                }
                StreamSchema::from_schema_keys(
                    Arc::new(Schema::new(fields)),
                    (1..=exprs.len()).collect(),
                )
                .unwrap()
            }
        }
    }
}

impl UserDefinedLogicalNodeCore for KeyCalculationExtension {
    fn name(&self) -> &str {
        KEY_CALCULATION_NAME
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "KeyCalculationExtension: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }

        let keys = match &self.keys {
            KeysOrExprs::Keys(k) => KeysOrExprs::Keys(k.clone()),
            KeysOrExprs::Exprs(_) => KeysOrExprs::Exprs(exprs),
        };

        Ok(Self {
            name: self.name.clone(),
            input: inputs[0].clone(),
            keys,
            schema: self.schema.clone(),
        })
    }
}
