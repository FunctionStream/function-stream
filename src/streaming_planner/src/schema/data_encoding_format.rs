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

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, plan_err};

use super::column_descriptor::ColumnDescriptor;
use crate::common::Format;
use crate::common::constants::cdc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DataEncodingFormat {
    #[default]
    Raw,
    StandardJson,
    DebeziumJson,
    Avro,
    Parquet,
    Csv,
    JsonL,
    Orc,
    Protobuf,
}

impl DataEncodingFormat {
    pub fn from_format(format: Option<&Format>) -> Self {
        match format {
            Some(Format::Json(j)) if j.debezium => Self::DebeziumJson,
            Some(Format::Json(_)) => Self::StandardJson,
            Some(Format::Avro(_)) => Self::Avro,
            Some(Format::Parquet(_)) => Self::Parquet,
            Some(Format::Csv(_)) => Self::Csv,
            Some(Format::Protobuf(_)) => Self::Protobuf,
            Some(Format::RawString(_)) | Some(Format::RawBytes(_)) | None => Self::Raw,
            Some(_) => Self::Raw,
        }
    }

    pub fn is_cdc_format(&self) -> bool {
        matches!(self, Self::DebeziumJson)
    }

    #[inline]
    pub fn supports_delta_updates(&self) -> bool {
        self.is_cdc_format()
    }

    pub fn apply_envelope(
        &self,
        logical_columns: Vec<ColumnDescriptor>,
    ) -> Result<Vec<ColumnDescriptor>> {
        if !self.is_cdc_format() {
            return Ok(logical_columns);
        }

        if logical_columns.is_empty() {
            return Ok(logical_columns);
        }

        if logical_columns.iter().any(|c| c.is_computed()) {
            return plan_err!(
                "Computed/Virtual columns are not supported directly inside a CDC source table; \
                 define computed columns in a downstream VIEW or AS SELECT streaming query"
            );
        }

        let inner_fields: Vec<Field> = logical_columns
            .into_iter()
            .map(|c| c.into_arrow_field())
            .collect();

        let row_struct_type = DataType::Struct(inner_fields.into());

        Ok(vec![
            ColumnDescriptor::new_physical(Field::new(cdc::BEFORE, row_struct_type.clone(), true)),
            ColumnDescriptor::new_physical(Field::new(cdc::AFTER, row_struct_type, true)),
            ColumnDescriptor::new_physical(Field::new(cdc::OP, DataType::Utf8, true)),
        ])
    }
}
