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

use datafusion::arrow::datatypes::{
    DECIMAL_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION, DataType, Field, IntervalUnit, TimeUnit,
};
use datafusion::common::{Result, plan_datafusion_err, plan_err};

use crate::sql::common::constants::planning_placeholder_udf;
use crate::sql::common::FsExtensionType;

pub fn convert_data_type(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> Result<(DataType, Option<FsExtensionType>)> {
    use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
    use datafusion::sql::sqlparser::ast::DataType as SQLDataType;

    match sql_type {
        SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type))
        | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_sql_type, _)) => {
            let (data_type, extension) = convert_simple_data_type(inner_sql_type)?;

            Ok((
                DataType::List(Arc::new(FsExtensionType::add_metadata(
                    extension,
                    Field::new(planning_placeholder_udf::LIST_ELEMENT_FIELD, data_type, true),
                ))),
                None,
            ))
        }
        SQLDataType::Array(ArrayElemTypeDef::None) => {
            plan_err!("Arrays with unspecified type is not supported")
        }
        other => convert_simple_data_type(other),
    }
}

fn convert_simple_data_type(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> Result<(DataType, Option<FsExtensionType>)> {
    use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
    use datafusion::sql::sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

    if matches!(sql_type, SQLDataType::JSON) {
        return Ok((DataType::Utf8, Some(FsExtensionType::JSON)));
    }

    let dt = match sql_type {
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
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
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
                return plan_err!(
                    "unsupported precision {} -- supported precisions are 0 (seconds), \
            3 (milliseconds), 6 (microseconds), and 9 (nanoseconds)",
                    precision
                );
            }
        },
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                return plan_err!("Unsupported SQL type {sql_type:?}");
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
                        convert_data_type(&f.field_type)?.0,
                        true,
                    )))
                })
                .collect::<Result<_>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => return plan_err!("Unsupported SQL type {sql_type:?}"),
    };

    Ok((dt?, None))
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
