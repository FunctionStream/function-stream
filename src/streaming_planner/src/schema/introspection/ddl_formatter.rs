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

use std::collections::BTreeMap;
use std::fmt::{self, Write};

use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};

use crate::common::constants::sql_field;

pub struct DdlBuilder<'a> {
    table_name: &'a str,
    schema: &'a Schema,
    watermark_column: Option<&'a str>,
    primary_keys: &'a [String],
    options: BTreeMap<String, String>,
}

impl<'a> DdlBuilder<'a> {
    pub fn new(table_name: &'a str, schema: &'a Schema) -> Self {
        Self {
            table_name,
            schema,
            watermark_column: None,
            primary_keys: &[],
            options: BTreeMap::new(),
        }
    }

    pub fn with_watermark(mut self, watermark: Option<&'a str>) -> Self {
        self.watermark_column = watermark;
        self
    }

    pub fn with_primary_keys(mut self, keys: &'a [String]) -> Self {
        self.primary_keys = keys;
        self
    }

    pub fn with_options(
        mut self,
        opts: &BTreeMap<String, String>,
        role: &str,
        connector: &str,
    ) -> Self {
        self.options = opts.clone();
        self.options
            .entry("type".to_string())
            .or_insert_with(|| role.to_string());
        self.options
            .entry("connector".to_string())
            .or_insert_with(|| connector.to_string());
        self
    }
}

impl<'a> fmt::Display for DdlBuilder<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "CREATE TABLE {} (", self.table_name)?;

        let mut rows: Vec<String> = Vec::new();
        for field in self.schema.fields() {
            let null_constraint = if field.is_nullable() { "" } else { " NOT NULL" };
            rows.push(format!(
                "  {} {}{}",
                field.name(),
                format_data_type(field.data_type()),
                null_constraint
            ));
        }

        if let Some(wm) = self.watermark_column
            && wm != sql_field::COMPUTED_WATERMARK
        {
            rows.push(format!("  WATERMARK FOR {wm}"));
        }

        if !self.primary_keys.is_empty() {
            rows.push(format!("  PRIMARY KEY ({})", self.primary_keys.join(", ")));
        }

        writeln!(f, "{}", rows.join(",\n"))?;
        write!(f, ")")?;

        if !self.options.is_empty() {
            writeln!(f)?;
            writeln!(f, "WITH (")?;
            let mut opt_lines: Vec<String> = Vec::with_capacity(self.options.len());
            for (k, v) in &self.options {
                let k_esc = k.replace('\'', "''");
                let v_esc = v.replace('\'', "''");
                opt_lines.push(format!("  '{k_esc}' = '{v_esc}'"));
            }
            write!(f, "{}\n);", opt_lines.join(",\n"))?;
        } else {
            write!(f, ";")?;
        }

        Ok(())
    }
}

pub fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 => "FLOAT".to_string(),
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".to_string(),
        DataType::Binary | DataType::LargeBinary => "VARBINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(unit, tz) => match (unit, tz) {
            (TimeUnit::Second, None) => "TIMESTAMP(0)".to_string(),
            (TimeUnit::Millisecond, None) => "TIMESTAMP(3)".to_string(),
            (TimeUnit::Microsecond, None) => "TIMESTAMP(6)".to_string(),
            (TimeUnit::Nanosecond, None) => "TIMESTAMP(9)".to_string(),
            (_, Some(_)) => "TIMESTAMP WITH TIME ZONE".to_string(),
        },
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({p}, {s})"),
        _ => dt.to_string(),
    }
}

pub fn schema_columns_one_line(schema: &Schema) -> String {
    let mut buf = String::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx > 0 {
            buf.push_str(", ");
        }
        let _ = write!(
            buf,
            "{}:{}",
            field.name(),
            format_data_type(field.data_type())
        );
    }
    buf
}
