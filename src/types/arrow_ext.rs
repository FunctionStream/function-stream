use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::SystemTime;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

pub struct DisplayAsSql<'a>(pub &'a DataType);

impl Display for DisplayAsSql<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int8 | DataType::Int16 | DataType::Int32 => write!(f, "INT"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => write!(f, "INT UNSIGNED"),
            DataType::UInt64 => write!(f, "BIGINT UNSIGNED"),
            DataType::Float16 | DataType::Float32 => write!(f, "FLOAT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Timestamp(_, _) => write!(f, "TIMESTAMP"),
            DataType::Date32 => write!(f, "DATE"),
            DataType::Date64 => write!(f, "DATETIME"),
            DataType::Time32(_) => write!(f, "TIME"),
            DataType::Time64(_) => write!(f, "TIME"),
            DataType::Duration(_) => write!(f, "INTERVAL"),
            DataType::Interval(_) => write!(f, "INTERVAL"),
            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => {
                write!(f, "BYTEA")
            }
            DataType::Utf8 | DataType::LargeUtf8 => write!(f, "TEXT"),
            DataType::List(inner) => {
                write!(f, "{}[]", DisplayAsSql(inner.data_type()))
            }
            dt => write!(f, "{dt}"),
        }
    }
}

/// Arrow extension type markers for FunctionStream-specific semantics.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum FsExtensionType {
    JSON,
}

impl FsExtensionType {
    pub fn from_map(map: &HashMap<String, String>) -> Option<Self> {
        match map.get("ARROW:extension:name")?.as_str() {
            "functionstream.json" => Some(Self::JSON),
            _ => None,
        }
    }

    pub fn add_metadata(v: Option<Self>, field: Field) -> Field {
        if let Some(v) = v {
            let mut m = HashMap::new();
            match v {
                FsExtensionType::JSON => {
                    m.insert(
                        "ARROW:extension:name".to_string(),
                        "functionstream.json".to_string(),
                    );
                }
            }
            field.with_metadata(m)
        } else {
            field
        }
    }
}

pub trait GetArrowType {
    fn arrow_type() -> DataType;
}

pub trait GetArrowSchema {
    fn arrow_schema() -> datafusion::arrow::datatypes::Schema;
}

impl<T> GetArrowType for T
where
    T: GetArrowSchema,
{
    fn arrow_type() -> DataType {
        DataType::Struct(Self::arrow_schema().fields.clone())
    }
}

impl GetArrowType for bool {
    fn arrow_type() -> DataType {
        DataType::Boolean
    }
}

impl GetArrowType for i8 {
    fn arrow_type() -> DataType {
        DataType::Int8
    }
}

impl GetArrowType for i16 {
    fn arrow_type() -> DataType {
        DataType::Int16
    }
}

impl GetArrowType for i32 {
    fn arrow_type() -> DataType {
        DataType::Int32
    }
}

impl GetArrowType for i64 {
    fn arrow_type() -> DataType {
        DataType::Int64
    }
}

impl GetArrowType for u8 {
    fn arrow_type() -> DataType {
        DataType::UInt8
    }
}

impl GetArrowType for u16 {
    fn arrow_type() -> DataType {
        DataType::UInt16
    }
}

impl GetArrowType for u32 {
    fn arrow_type() -> DataType {
        DataType::UInt32
    }
}

impl GetArrowType for u64 {
    fn arrow_type() -> DataType {
        DataType::UInt64
    }
}

impl GetArrowType for f32 {
    fn arrow_type() -> DataType {
        DataType::Float32
    }
}

impl GetArrowType for f64 {
    fn arrow_type() -> DataType {
        DataType::Float64
    }
}

impl GetArrowType for String {
    fn arrow_type() -> DataType {
        DataType::Utf8
    }
}

impl GetArrowType for Vec<u8> {
    fn arrow_type() -> DataType {
        DataType::Binary
    }
}

impl GetArrowType for SystemTime {
    fn arrow_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }
}
