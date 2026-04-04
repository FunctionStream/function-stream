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
use arrow_array::builder::{BinaryBuilder, StringBuilder, TimestampNanosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_json::reader::ReaderBuilder;
use arrow_schema::{Schema, SchemaRef};
use std::sync::Arc;
use tracing::warn;

use super::config::{BadDataPolicy, Format};
use crate::sql::common::TIMESTAMP_FIELD;

pub struct DataDeserializer {
    format: Format,
    final_schema: SchemaRef,
    decoder_schema: SchemaRef,
    bad_data_policy: BadDataPolicy,
}

impl DataDeserializer {
    pub fn new(format: Format, schema: SchemaRef, bad_data_policy: BadDataPolicy) -> Self {
        let decoder_schema = schema_without_timestamp(schema.as_ref());
        Self {
            format,
            final_schema: schema,
            decoder_schema,
            bad_data_policy,
        }
    }

    pub fn deserialize_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch> {
        self.deserialize_batch_with_kafka_timestamps(messages, &[])
    }

    pub fn deserialize_batch_with_kafka_timestamps(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        if messages.is_empty() {
            return Ok(RecordBatch::new_empty(self.final_schema.clone()));
        }

        if !kafka_timestamps_ms.is_empty() && kafka_timestamps_ms.len() != messages.len() {
            warn!(
                message_count = messages.len(),
                timestamp_count = kafka_timestamps_ms.len(),
                "Kafka timestamps count mismatches with messages count"
            );
        }

        match &self.format {
            Format::Json(_) => self
                .deserialize_json(messages, kafka_timestamps_ms)
                .context("Failed to deserialize JSON format"),
            Format::RawString => self
                .deserialize_raw_string(messages, kafka_timestamps_ms)
                .context("Failed to deserialize RawString format"),
            Format::RawBytes => self
                .deserialize_raw_bytes(messages, kafka_timestamps_ms)
                .context("Failed to deserialize RawBytes format"),
        }
    }

    fn deserialize_json(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        let total_capacity: usize = messages.iter().map(|m| m.len() + 1).sum();
        let mut buffer = Vec::with_capacity(total_capacity);
        for msg in messages {
            buffer.extend_from_slice(msg);
            buffer.push(b'\n');
        }

        let mut decoder = ReaderBuilder::new(self.decoder_schema.clone())
            .with_strict_mode(false)
            .build_decoder()
            .context("Failed to build Arrow JSON decoder")?;

        decoder
            .decode(&buffer)
            .context("Arrow JSON decoding error")?;

        match self.bad_data_policy {
            BadDataPolicy::Drop => {
                let Some((batch, mask, _remainder, _errors)) = decoder.flush_with_bad_data()?
                else {
                    return Ok(RecordBatch::new_empty(self.final_schema.clone()));
                };

                let valid_count = mask.true_count();
                if valid_count < messages.len() {
                    warn!(
                        "Dropped {} malformed JSON rows",
                        messages.len() - valid_count
                    );
                }

                let valid_indices: Vec<usize> =
                    (0..mask.len()).filter(|&i| mask.value(i)).collect();
                self.rebuild_with_timestamp(batch, kafka_timestamps_ms, &valid_indices)
            }
            _ => {
                let batch = decoder
                    .flush()?
                    .unwrap_or_else(|| RecordBatch::new_empty(self.decoder_schema.clone()));
                let indices: Vec<usize> = (0..batch.num_rows()).collect();
                self.rebuild_with_timestamp(batch, kafka_timestamps_ms, &indices)
            }
        }
    }

    fn deserialize_raw_string(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        let value_idx = self
            .decoder_schema
            .index_of("value")
            .context("RawString format requires a 'value' column in the schema")?;
        let total_bytes: usize = messages.iter().map(|m| m.len()).sum();
        let mut builder = StringBuilder::with_capacity(messages.len(), total_bytes);

        for msg in messages {
            builder.append_value(String::from_utf8_lossy(msg));
        }

        let mut columns = vec![None; self.decoder_schema.fields().len()];
        columns[value_idx] = Some(Arc::new(builder.finish()) as ArrayRef);

        let decoded_columns = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                c.ok_or_else(|| anyhow!("Missing decoded column at index {} for RawString", i))
            })
            .collect::<Result<Vec<_>>>()?;

        let decoded_batch = RecordBatch::try_new(self.decoder_schema.clone(), decoded_columns)
            .context("Failed to build RecordBatch for RawString")?;

        let valid_indices: Vec<usize> = (0..decoded_batch.num_rows()).collect();
        self.rebuild_with_timestamp(decoded_batch, kafka_timestamps_ms, &valid_indices)
    }

    fn deserialize_raw_bytes(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        let value_idx = self
            .decoder_schema
            .index_of("value")
            .context("RawBytes format requires a 'value' column in the schema")?;

        let total_bytes: usize = messages.iter().map(|m| m.len()).sum();
        let mut builder = BinaryBuilder::with_capacity(messages.len(), total_bytes);

        for msg in messages {
            builder.append_value(msg);
        }

        let mut columns = vec![None; self.decoder_schema.fields().len()];
        columns[value_idx] = Some(Arc::new(builder.finish()) as ArrayRef);

        let decoded_columns = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                c.ok_or_else(|| anyhow!("Missing decoded column at index {} for RawBytes", i))
            })
            .collect::<Result<Vec<_>>>()?;

        let decoded_batch = RecordBatch::try_new(self.decoder_schema.clone(), decoded_columns)
            .context("Failed to build RecordBatch for RawBytes")?;

        let valid_indices: Vec<usize> = (0..decoded_batch.num_rows()).collect();
        self.rebuild_with_timestamp(decoded_batch, kafka_timestamps_ms, &valid_indices)
    }

    fn rebuild_with_timestamp(
        &self,
        decoded_batch: RecordBatch,
        kafka_timestamps_ms: &[u64],
        valid_indices: &[usize],
    ) -> Result<RecordBatch> {
        let num_rows = valid_indices.len();

        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        for &idx in valid_indices {
            let ms = kafka_timestamps_ms.get(idx).copied().unwrap_or(0);
            ts_builder.append_value((ms as i64).saturating_mul(1_000_000));
        }
        let timestamp_col: ArrayRef = Arc::new(ts_builder.finish());

        let mut columns = Vec::with_capacity(self.final_schema.fields().len());
        for field in self.final_schema.fields() {
            if field.name() == TIMESTAMP_FIELD {
                columns.push(timestamp_col.clone());
            } else {
                let idx = self
                    .decoder_schema
                    .index_of(field.name())
                    .map_err(|_| anyhow!("Field '{}' missing in decoded batch", field.name()))?;
                columns.push(decoded_batch.column(idx).clone());
            }
        }

        RecordBatch::try_new(self.final_schema.clone(), columns)
            .context("Final RecordBatch assembly with timestamp failed")
    }
}

fn schema_without_timestamp(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .filter(|f| f.name() != TIMESTAMP_FIELD)
        .cloned()
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
