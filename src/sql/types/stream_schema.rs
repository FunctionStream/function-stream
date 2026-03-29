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

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::Result;

use super::TIMESTAMP_FIELD;

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
