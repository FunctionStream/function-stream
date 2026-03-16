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
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult, TableReference};

use crate::sql::types::{DFField, TIMESTAMP_FIELD};

/// Returns the Arrow struct type for a window (start, end) pair.
pub fn window_arrow_struct() -> DataType {
    DataType::Struct(
        vec![
            Arc::new(Field::new(
                "start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(Field::new(
                "end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
        ]
        .into(),
    )
}

/// Adds a `_timestamp` field to a DFSchema if it doesn't already have one.
pub fn add_timestamp_field(
    schema: DFSchemaRef,
    qualifier: Option<TableReference>,
) -> DFResult<DFSchemaRef> {
    if has_timestamp_field(&schema) {
        return Ok(schema);
    }

    let timestamp_field = DFField::new(
        qualifier,
        TIMESTAMP_FIELD,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    );
    Ok(Arc::new(schema.join(&DFSchema::new_with_metadata(
        vec![timestamp_field.into()],
        HashMap::new(),
    )?)?))
}

/// Checks whether a DFSchema contains a `_timestamp` field.
pub fn has_timestamp_field(schema: &DFSchemaRef) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| field.name() == TIMESTAMP_FIELD)
}

/// Adds a `_timestamp` field to an Arrow Schema, returning a new SchemaRef.
pub fn add_timestamp_field_arrow(schema: Schema) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        TIMESTAMP_FIELD,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )));
    Arc::new(Schema::new(fields))
}
