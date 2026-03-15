use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{
    DECIMAL_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION, DataType, Field, FieldRef, IntervalUnit,
    Schema, SchemaRef, TimeUnit,
};
use datafusion::common::{Column, DFSchema, Result, TableReference, plan_datafusion_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;

pub const TIMESTAMP_FIELD: &str = "_timestamp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessingMode {
    Append,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WindowType {
    Tumbling { width: Duration },
    Sliding { width: Duration, slide: Duration },
    Session { gap: Duration },
    Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum WindowBehavior {
    FromOperator {
        window: WindowType,
        window_field: DFField,
        window_index: usize,
        is_nested: bool,
    },
    InData,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DFField {
    qualifier: Option<TableReference>,
    field: FieldRef,
}

impl From<(Option<TableReference>, FieldRef)> for DFField {
    fn from(value: (Option<TableReference>, FieldRef)) -> Self {
        Self {
            qualifier: value.0,
            field: value.1,
        }
    }
}

impl From<(Option<&TableReference>, &Field)> for DFField {
    fn from(value: (Option<&TableReference>, &Field)) -> Self {
        Self {
            qualifier: value.0.cloned(),
            field: Arc::new(value.1.clone()),
        }
    }
}

impl From<DFField> for (Option<TableReference>, FieldRef) {
    fn from(value: DFField) -> Self {
        (value.qualifier, value.field)
    }
}

impl DFField {
    pub fn new(
        qualifier: Option<TableReference>,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            qualifier,
            field: Arc::new(Field::new(name, data_type, nullable)),
        }
    }

    pub fn new_unqualified(name: &str, data_type: DataType, nullable: bool) -> Self {
        DFField {
            qualifier: None,
            field: Arc::new(Field::new(name, data_type, nullable)),
        }
    }

    pub fn name(&self) -> &String {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        self.field.metadata()
    }

    pub fn qualified_name(&self) -> String {
        if let Some(qualifier) = &self.qualifier {
            format!("{}.{}", qualifier, self.field.name())
        } else {
            self.field.name().to_owned()
        }
    }

    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.field.name().to_string(),
            spans: Default::default(),
        }
    }

    pub fn unqualified_column(&self) -> Column {
        Column {
            relation: None,
            name: self.field.name().to_string(),
            spans: Default::default(),
        }
    }

    pub fn qualifier(&self) -> Option<&TableReference> {
        self.qualifier.as_ref()
    }

    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    pub fn strip_qualifier(mut self) -> Self {
        self.qualifier = None;
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        let f = self.field().as_ref().clone().with_nullable(nullable);
        self.field = f.into();
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        let f = self.field().as_ref().clone().with_metadata(metadata);
        self.field = f.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSchema {
    pub schema: SchemaRef,
    pub timestamp_index: usize,
    pub key_indices: Option<Vec<usize>>,
}

impl StreamSchema {
    pub fn new(schema: SchemaRef, timestamp_index: usize, key_indices: Option<Vec<usize>>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices,
        }
    }

    pub fn new_unkeyed(schema: SchemaRef, timestamp_index: usize) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: None,
        }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        let schema = Arc::new(Schema::new(fields));
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .map(|(i, _)| i)
            .unwrap_or(0);
        Self {
            schema,
            timestamp_index,
            key_indices: None,
        }
    }

    pub fn from_schema_keys(schema: SchemaRef, key_indices: Vec<usize>) -> Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema, schema is {schema:?}"
                ))
            })?
            .0;
        Ok(Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
        })
    }

    pub fn from_schema_unkeyed(schema: SchemaRef) -> Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "no {TIMESTAMP_FIELD} field in schema"
                ))
            })?
            .0;
        Ok(Self {
            schema,
            timestamp_index,
            key_indices: None,
        })
    }
}

#[allow(clippy::type_complexity)]
pub(crate) struct PlaceholderUdf {
    name: String,
    signature: Signature,
    return_type: Arc<dyn Fn(&[DataType]) -> Result<DataType> + Send + Sync + 'static>,
}

impl Debug for PlaceholderUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlaceholderUDF<{}>", self.name)
    }
}

impl ScalarUDFImpl for PlaceholderUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        (self.return_type)(args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unimplemented!("PlaceholderUdf should never be called at execution time");
    }
}

impl PlaceholderUdf {
    pub fn with_return(
        name: impl Into<String>,
        args: Vec<DataType>,
        ret: DataType,
    ) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::new_from_impl(PlaceholderUdf {
            name: name.into(),
            signature: Signature::exact(args, Volatility::Volatile),
            return_type: Arc::new(move |_| Ok(ret.clone())),
        }))
    }
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
        }
    }
}

#[derive(Clone)]
pub struct PlanningOptions {
    pub ttl: Duration,
}

impl Default for PlanningOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(24 * 60 * 60),
        }
    }
}

pub fn convert_data_type(sql_type: &datafusion::sql::sqlparser::ast::DataType) -> Result<DataType> {
    use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
    use datafusion::sql::sqlparser::ast::DataType as SQLDataType;

    match sql_type {
        SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type))
        | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_sql_type, _)) => {
            let data_type = convert_data_type(inner_sql_type)?;
            Ok(DataType::List(Arc::new(Field::new(
                "field", data_type, true,
            ))))
        }
        SQLDataType::Array(ArrayElemTypeDef::None) => {
            plan_err!("Arrays with unspecified type is not supported")
        }
        other => convert_simple_data_type(other),
    }
}

fn convert_simple_data_type(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> Result<DataType> {
    use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
    use datafusion::sql::sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

    match sql_type {
        SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
        SQLDataType::TinyInt(_) => Ok(DataType::Int8),
        SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(DataType::Int64),
        SQLDataType::TinyIntUnsigned(_) => Ok(DataType::UInt8),
        SQLDataType::SmallIntUnsigned(_) | SQLDataType::Int2Unsigned(_) => Ok(DataType::UInt16),
        SQLDataType::IntUnsigned(_)
        | SQLDataType::UnsignedInteger
        | SQLDataType::Int4Unsigned(_) => Ok(DataType::UInt32),
        SQLDataType::BigIntUnsigned(_) | SQLDataType::Int8Unsigned(_) => Ok(DataType::UInt64),
        SQLDataType::Float(_) | SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
        SQLDataType::Double(_) | SQLDataType::DoublePrecision | SQLDataType::Float8 => {
            Ok(DataType::Float64)
        }
        SQLDataType::Char(_)
        | SQLDataType::Varchar(_)
        | SQLDataType::Text
        | SQLDataType::String(_) => Ok(DataType::Utf8),
        SQLDataType::Timestamp(None, TimezoneInfo::None) | SQLDataType::Datetime(_) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        SQLDataType::Timestamp(Some(precision), TimezoneInfo::None) => match *precision {
            0 => Ok(DataType::Timestamp(TimeUnit::Second, None)),
            3 => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
            6 => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            9 => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => {
                plan_err!(
                    "unsupported precision {} -- supported: 0 (seconds), 3 (ms), 6 (us), 9 (ns)",
                    precision
                )
            }
        },
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                plan_err!("Unsupported SQL type {sql_type:?}")
            }
        }
        SQLDataType::Numeric(exact_number_info) | SQLDataType::Decimal(exact_number_info) => {
            let (precision, scale) = match *exact_number_info {
                ExactNumberInfo::None => (None, None),
                ExactNumberInfo::Precision(precision) => (Some(precision), None),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision), Some(scale))
                }
            };
            make_decimal_type(precision, scale)
        }
        SQLDataType::Bytea => Ok(DataType::Binary),
        SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        SQLDataType::Struct(fields, _) => {
            let fields: Vec<_> = fields
                .iter()
                .map(|f| {
                    Ok::<_, datafusion::error::DataFusionError>(Arc::new(Field::new(
                        f.field_name
                            .as_ref()
                            .ok_or_else(|| {
                                plan_datafusion_err!("anonymous struct fields are not allowed")
                            })?
                            .to_string(),
                        convert_data_type(&f.field_type)?,
                        true,
                    )))
                })
                .collect::<Result<_>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => plan_err!("Unsupported SQL type {sql_type:?}"),
    }
}

fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => return plan_err!("Cannot specify only scale for decimal data type"),
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        plan_err!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

pub fn fields_with_qualifiers(schema: &DFSchema) -> Vec<DFField> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (schema.qualified_field(i).0.cloned(), f.clone()).into())
        .collect()
}

pub fn schema_from_df_fields(fields: &[DFField]) -> Result<DFSchema> {
    schema_from_df_fields_with_metadata(fields, HashMap::new())
}

pub fn schema_from_df_fields_with_metadata(
    fields: &[DFField],
    metadata: HashMap<String, String>,
) -> Result<DFSchema> {
    DFSchema::new_with_metadata(fields.iter().map(|t| t.clone().into()).collect(), metadata)
}

pub fn get_duration(expression: &Expr) -> Result<Duration> {
    use datafusion::common::ScalarValue;

    match expression {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(val)), _) => {
            Ok(Duration::from_secs((val.days as u64) * 24 * 60 * 60)
                + Duration::from_millis(val.milliseconds as u64))
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val)), _) => {
            if val.months != 0 {
                return datafusion::common::not_impl_err!(
                    "Windows do not support durations specified as months"
                );
            }
            Ok(Duration::from_secs((val.days as u64) * 24 * 60 * 60)
                + Duration::from_nanos(val.nanoseconds as u64))
        }
        _ => plan_err!(
            "unsupported Duration expression, expect duration literal, not {}",
            expression
        ),
    }
}

pub fn find_window(expression: &Expr) -> Result<Option<WindowType>> {
    use datafusion::logical_expr::expr::Alias;
    use datafusion::logical_expr::expr::ScalarFunction;

    match expression {
        Expr::ScalarFunction(ScalarFunction { func: fun, args }) => match fun.name() {
            "hop" => {
                if args.len() != 2 {
                    unreachable!();
                }
                let slide = get_duration(&args[0])?;
                let width = get_duration(&args[1])?;
                if width.as_nanos() % slide.as_nanos() != 0 {
                    return plan_err!(
                        "hop() width {:?} must be a multiple of slide {:?}",
                        width,
                        slide
                    );
                }
                if slide == width {
                    Ok(Some(WindowType::Tumbling { width }))
                } else {
                    Ok(Some(WindowType::Sliding { width, slide }))
                }
            }
            "tumble" => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for tumble(), expect one");
                }
                let width = get_duration(&args[0])?;
                Ok(Some(WindowType::Tumbling { width }))
            }
            "session" => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for session(), expected one");
                }
                let gap = get_duration(&args[0])?;
                Ok(Some(WindowType::Session { gap }))
            }
            _ => Ok(None),
        },
        Expr::Alias(Alias { expr, .. }) => find_window(expr),
        _ => Ok(None),
    }
}
