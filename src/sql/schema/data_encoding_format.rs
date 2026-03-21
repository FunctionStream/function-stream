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

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, plan_err};

use super::column_descriptor::ColumnDescriptor;
use crate::sql::common::Format;

/// High-level payload encoding (orthogonal to `Format` wire details in `ConnectionSchema`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataEncodingFormat {
    StandardJson,
    DebeziumJson,
    Avro,
    Parquet,
    Raw,
}

impl DataEncodingFormat {
    pub fn extract_from_map(opts: &HashMap<String, String>) -> Result<Self> {
        let format_str = opts.get("format").map(|s| s.as_str()).unwrap_or("json");
        let is_debezium = opts
            .get("format.debezium")
            .or_else(|| opts.get("json.debezium"))
            .map(|s| s == "true")
            .unwrap_or(false);

        match (format_str, is_debezium) {
            ("json", true) | ("debezium_json", _) => Ok(Self::DebeziumJson),
            ("json", false) => Ok(Self::StandardJson),
            ("avro", _) => Ok(Self::Avro),
            ("parquet", _) => Ok(Self::Parquet),
            _ => Ok(Self::Raw),
        }
    }

    pub fn from_connection_format(format: &Format) -> Self {
        match format {
            Format::Json(j) if j.debezium => Self::DebeziumJson,
            Format::Json(_) => Self::StandardJson,
            Format::Avro(_) => Self::Avro,
            Format::Parquet(_) => Self::Parquet,
            Format::Protobuf(_) | Format::RawString(_) | Format::RawBytes(_) => Self::Raw,
        }
    }

    pub fn supports_delta_updates(&self) -> bool {
        matches!(self, Self::DebeziumJson)
    }

    pub fn apply_envelope(self, columns: Vec<ColumnDescriptor>) -> Result<Vec<ColumnDescriptor>> {
        if !self.supports_delta_updates() {
            return Ok(columns);
        }
        if columns.iter().any(|c| c.is_computed()) {
            return plan_err!("Virtual fields are not supported with CDC envelope");
        }
        if columns.is_empty() {
            return Ok(columns);
        }
        let fields: Vec<Field> = columns.into_iter().map(|c| c.into_arrow_field()).collect();
        let struct_type = DataType::Struct(fields.into());

        Ok(vec![
            ColumnDescriptor::new_physical(Field::new("before", struct_type.clone(), true)),
            ColumnDescriptor::new_physical(Field::new("after", struct_type.clone(), true)),
            ColumnDescriptor::new_physical(Field::new("op", DataType::Utf8, true)),
        ])
    }
}
