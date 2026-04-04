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
use datafusion::common::{DataFusionError, Result};

use super::TIMESTAMP_FIELD;

// ============================================================================
// StreamSchema
// ============================================================================

/// Schema wrapper for continuous streaming: requires event-time (`TIMESTAMP_FIELD`) for watermarks
/// and optionally tracks key column indices for partitioned state / shuffle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSchema {
    schema: SchemaRef,
    timestamp_index: usize,
    key_indices: Option<Vec<usize>>,
}

impl StreamSchema {
    // ========================================================================
    // Raw Constructors (When indices are strictly known in advance)
    // ========================================================================

    /// Keyed stream when indices are already verified.
    pub fn new_keyed(schema: SchemaRef, timestamp_index: usize, key_indices: Vec<usize>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
        }
    }

    /// Unkeyed stream when `timestamp_index` is already verified.
    pub fn new_unkeyed(schema: SchemaRef, timestamp_index: usize) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: None,
        }
    }

    // ========================================================================
    // Safe Builders (Dynamically resolves and validates indices)
    // ========================================================================

    /// Unkeyed stream from a field list. Replaces the old `unwrap_or(0)` default when the timestamp
    /// column was missing (silent wrong index / corruption).
    pub fn try_from_fields(fields: impl Into<Vec<Field>>) -> Result<Self> {
        let schema = Arc::new(Schema::new(fields.into()));
        Self::try_from_schema_unkeyed(schema)
    }

    /// Keyed stream from `SchemaRef`; resolves and validates the mandatory timestamp column.
    pub fn try_from_schema_keyed(schema: SchemaRef, key_indices: Vec<usize>) -> Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Streaming Topology Error: Mandatory event-time field '{}' is missing in the schema. \
                    Current schema fields: {:?}",
                    TIMESTAMP_FIELD,
                    schema.fields()
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
        })
    }

    /// Unkeyed stream from `SchemaRef`; resolves and validates the mandatory timestamp column.
    pub fn try_from_schema_unkeyed(schema: SchemaRef) -> Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Streaming Topology Error: Mandatory event-time field '{}' is missing.",
                    TIMESTAMP_FIELD
                ))
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: None,
        })
    }

    // ========================================================================
    // Zero-cost Getters
    // ========================================================================

    /// Underlying Arrow schema.
    #[inline]
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Physical column index used as event time / watermark driver.
    #[inline]
    pub fn timestamp_index(&self) -> usize {
        self.timestamp_index
    }

    /// Key column indices for shuffle / state, if keyed.
    #[inline]
    pub fn key_indices(&self) -> Option<&[usize]> {
        self.key_indices.as_deref()
    }

    #[inline]
    pub fn is_keyed(&self) -> bool {
        self.key_indices.is_some()
    }
}
