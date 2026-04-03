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

use anyhow::{Context, Result, anyhow};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_json::EncoderOptions;
use arrow_json::writer::make_encoder;
use arrow_schema::{Field, SchemaRef};
use std::sync::Arc;
use tracing::{debug, warn};

use super::config::{Format, JsonFormat};
use super::json_encoder::CustomEncoderFactory;
use crate::sql::common::TIMESTAMP_FIELD;

pub struct DataSerializer {
    format: Format,
    projection_indices: Vec<usize>,
}

impl DataSerializer {
    pub fn new(format: Format, schema: SchemaRef) -> Self {
        let projection_indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != TIMESTAMP_FIELD)
            .map(|(i, _)| i)
            .collect();

        Self {
            format,
            projection_indices,
        }
    }

    pub fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let projected_batch = batch
            .project(&self.projection_indices)
            .context("Failed to project RecordBatch (removing timestamp column)")?;

        match &self.format {
            Format::Json(config) => self
                .serialize_json(config, &projected_batch)
                .context("JSON serialization failed"),
            Format::RawString => self
                .serialize_raw_string(&projected_batch)
                .context("RawString serialization failed"),
            Format::RawBytes => self
                .serialize_raw_bytes(&projected_batch)
                .context("RawBytes serialization failed"),
        }
    }

    fn serialize_json(&self, config: &JsonFormat, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let num_rows = batch.num_rows();
        let array = StructArray::from(batch.clone());
        let field = Arc::new(Field::new_struct(
            "",
            batch.schema().fields().clone(),
            false,
        ));

        let encoder_factory = Arc::new(CustomEncoderFactory {
            timestamp_format: config.timestamp_format.clone(),
            decimal_encoding: config.decimal_encoding.clone(),
        });

        let options = EncoderOptions::default()
            .with_explicit_nulls(true)
            .with_encoder_factory(encoder_factory);

        let mut encoder =
            make_encoder(&field, &array, &options).context("Failed to build Arrow JSON encoder")?;

        let mut results = Vec::with_capacity(num_rows);

        let mut shared_buf = Vec::with_capacity(512);

        for idx in 0..num_rows {
            shared_buf.clear();
            encoder.encode(idx, &mut shared_buf);

            if !shared_buf.is_empty() {
                results.push(shared_buf.to_vec());
            } else {
                warn!(
                    row_index = idx,
                    "JSON encoder produced an empty buffer for row"
                );
            }
        }
        Ok(results)
    }

    fn serialize_raw_string(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let value_idx = batch
            .schema()
            .index_of("value")
            .context("RawString format requires a 'value' column in the schema")?;

        let string_array = batch
            .column(value_idx)
            .as_string_opt::<i32>()
            .context("RawString 'value' column is physically not a valid Utf8 Array")?;

        let num_rows = batch.num_rows();
        let mut results = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            if string_array.is_null(i) {
                results.push(Vec::new());
            } else {
                results.push(string_array.value(i).as_bytes().to_vec());
            }
        }

        Ok(results)
    }

    fn serialize_raw_bytes(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let value_idx = batch
            .schema()
            .index_of("value")
            .context("RawBytes format requires a 'value' column in the schema")?;

        let binary_array = batch
            .column(value_idx)
            .as_binary_opt::<i32>()
            .context("RawBytes 'value' column is physically not a valid Binary Array")?;

        let num_rows = batch.num_rows();
        let mut results = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            if binary_array.is_null(i) {
                results.push(Vec::new());
            } else {
                results.push(binary_array.value(i).to_vec());
            }
        }

        Ok(results)
    }
}
