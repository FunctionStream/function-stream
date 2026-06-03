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
use arrow::compute::concat_batches;
use arrow_array::builder::{BinaryBuilder, StringBuilder, TimestampNanosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_json::reader::ReaderBuilder;
use arrow_schema::{Schema, SchemaRef};
use std::sync::Arc;
use tracing::{debug, warn};

use super::config::{BadDataPolicy, Format};
use crate::sql::common::TIMESTAMP_FIELD;

/// `DataDeserializer` handles high-throughput message transformation
/// into Apache Arrow `RecordBatch`.
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

    /// High-performance entry point for batch deserialization.
    pub fn deserialize_batch_with_kafka_timestamps(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        if messages.is_empty() {
            return Ok(RecordBatch::new_empty(self.final_schema.clone()));
        }

        // Defensive check: align timestamps with messages
        let ts_len = kafka_timestamps_ms.len();
        let msg_len = messages.len();
        if ts_len > 0 && ts_len != msg_len {
            warn!(msg_len, ts_len, "Kafka timestamps count mismatch");
        }

        match &self.format {
            Format::Json(_) => self.deserialize_json(messages, kafka_timestamps_ms),
            Format::RawString => self.deserialize_raw_string(messages, kafka_timestamps_ms),
            Format::RawBytes => self.deserialize_raw_bytes(messages, kafka_timestamps_ms),
        }
    }

    /// JSON Deserialization with Row-Level Fault Tolerance.
    /// Performance Strategy: Uses an NDJSON (Newline Delimited JSON) approach
    /// but isolates malformed rows prior to full Arrow decoding.
    fn deserialize_json(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        let mut valid_messages = Vec::with_capacity(messages.len());
        let mut valid_indices = Vec::with_capacity(messages.len());
        let mut total_size = 0;

        // Step 1: Pre-scan for data quality (Fault Isolation)
        for (i, msg) in messages.iter().enumerate() {
            // Fast-path: Check if it's a valid JSON object/array without full binding
            if serde_json::from_slice::<serde_json::Value>(msg).is_ok() {
                valid_messages.push(*msg);
                valid_indices.push(i);
                total_size += msg.len() + 1; // +1 for newline
            } else {
                match self.bad_data_policy {
                    BadDataPolicy::Fail => {
                        return Err(anyhow!("Invalid JSON encountered at index {}", i));
                    }
                    BadDataPolicy::Drop => {
                        debug!(index = i, "Dropped malformed JSON row");
                        continue;
                    }
                }
            }
        }

        if valid_messages.is_empty() {
            return Ok(RecordBatch::new_empty(self.final_schema.clone()));
        }

        // Step 2: Batch Decode valid rows
        let mut buffer = Vec::with_capacity(total_size);
        for msg in valid_messages {
            buffer.extend_from_slice(msg);
            buffer.push(b'\n');
        }

        let decoded_batch =
            decode_ndjson_buffer(&self.decoder_schema, &buffer, valid_indices.len())?;

        // Step 3: Re-inject Event-Time Column
        self.rebuild_with_timestamp(decoded_batch, kafka_timestamps_ms, &valid_indices)
    }

    fn deserialize_raw_string(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        self.decoder_schema
            .index_of("value")
            .context("Schema must contain 'value' for RawString")?;

        let mut builder =
            StringBuilder::with_capacity(messages.len(), messages.iter().map(|m| m.len()).sum());
        for msg in messages {
            builder.append_value(String::from_utf8_lossy(msg));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.decoder_schema.fields().len());
        columns.push(Arc::new(builder.finish()));

        let decoded_batch = RecordBatch::try_new(self.decoder_schema.clone(), columns)?;
        let indices: Vec<usize> = (0..messages.len()).collect();

        self.rebuild_with_timestamp(decoded_batch, kafka_timestamps_ms, &indices)
    }

    fn deserialize_raw_bytes(
        &self,
        messages: &[&[u8]],
        kafka_timestamps_ms: &[u64],
    ) -> Result<RecordBatch> {
        self.decoder_schema
            .index_of("value")
            .context("Schema must contain 'value' for RawBytes")?;

        let mut builder =
            BinaryBuilder::with_capacity(messages.len(), messages.iter().map(|m| m.len()).sum());
        for msg in messages {
            builder.append_value(msg);
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.decoder_schema.fields().len());
        columns.push(Arc::new(builder.finish()));

        let decoded_batch = RecordBatch::try_new(self.decoder_schema.clone(), columns)?;
        let indices: Vec<usize> = (0..messages.len()).collect();

        self.rebuild_with_timestamp(decoded_batch, kafka_timestamps_ms, &indices)
    }

    /// Assembler: Merges the decoded Batch with external Event-Time (Watermark) data.
    fn rebuild_with_timestamp(
        &self,
        decoded_batch: RecordBatch,
        kafka_timestamps_ms: &[u64],
        valid_indices: &[usize],
    ) -> Result<RecordBatch> {
        let num_rows = decoded_batch.num_rows();

        // Safety check for indices
        if valid_indices.len() != num_rows {
            return Err(anyhow!(
                "Alignment error: valid rows ({}) != decoded rows ({})",
                valid_indices.len(),
                num_rows
            ));
        }

        // 1. Build Timestamp Column (Nanoseconds for Arrow standard)
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(num_rows);
        for &idx in valid_indices {
            let ms = kafka_timestamps_ms.get(idx).copied().unwrap_or(0);
            ts_builder.append_value((ms as i64).saturating_mul(1_000_000));
        }
        let timestamp_col: ArrayRef = Arc::new(ts_builder.finish());

        // 2. Final Assembly based on Target Schema
        let mut final_columns = Vec::with_capacity(self.final_schema.fields().len());
        for field in self.final_schema.fields() {
            if field.name() == TIMESTAMP_FIELD {
                final_columns.push(timestamp_col.clone());
            } else {
                let col = decoded_batch.column_by_name(field.name()).ok_or_else(|| {
                    anyhow!("Field '{}' not found in decoded batch", field.name())
                })?;
                final_columns.push(col.clone());
            }
        }

        RecordBatch::try_new(self.final_schema.clone(), final_columns)
            .context("Failed to assemble final RecordBatch with event-time")
    }
}

/// Decodes NDJSON bytes into a single [`RecordBatch`], looping decode/flush until the
/// buffer is fully consumed. Arrow's JSON decoder defaults to `batch_size = 1024`, so a
/// single decode+flush is insufficient when the input has more rows than that.
fn decode_ndjson_buffer(
    decoder_schema: &SchemaRef,
    buffer: &[u8],
    expected_rows: usize,
) -> Result<RecordBatch> {
    let mut decoder = ReaderBuilder::new(decoder_schema.clone())
        .with_strict_mode(false)
        .with_batch_size(expected_rows.max(1))
        .build_decoder()
        .context("Failed to build Arrow JSON decoder")?;

    let mut offset = 0;
    let mut batches = Vec::new();
    while offset < buffer.len() {
        let decoded = decoder
            .decode(&buffer[offset..])
            .context("Arrow batch decoding failed")?;
        if decoded == 0 {
            return Err(anyhow!(
                "Arrow JSON decoder stalled at offset {}/{}",
                offset,
                buffer.len()
            ));
        }
        offset += decoded;

        while let Some(batch) = decoder.flush()? {
            batches.push(batch);
        }
    }

    if batches.is_empty() {
        return Err(anyhow!(
            "Decoder returned empty batch after successful validation"
        ));
    }

    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        concat_batches(decoder_schema, &batches).context("Failed to concat decoded JSON batches")
    }
}

/// Helper: Strips the specialized timestamp field to allow the raw decoder
/// to focus only on payload data.
fn schema_without_timestamp(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .filter(|f| f.name() != TIMESTAMP_FIELD)
        .cloned()
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::config::{BadDataPolicy, DecimalEncoding, Format, JsonFormat, TimestampFormat};
    use arrow_schema::{DataType, Field, TimeUnit};

    fn test_deserializer() -> DataDeserializer {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        DataDeserializer::new(
            Format::Json(JsonFormat {
                timestamp_format: TimestampFormat::UnixMillis,
                decimal_encoding: DecimalEncoding::String,
                include_schema: false,
            }),
            schema,
            BadDataPolicy::Fail,
        )
    }

    #[test]
    fn decode_json_batch_exceeds_arrow_default_batch_size() {
        let deserializer = test_deserializer();
        let row_count = 2500;
        let messages: Vec<Vec<u8>> = (0..row_count)
            .map(|i| format!(r#"{{"id":{i},"value":"row-{i}"}}"#).into_bytes())
            .collect();
        let msg_refs: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
        let kafka_ts: Vec<u64> = (0..row_count).map(|i| 1_700_000_000_000 + i as u64).collect();

        let batch = deserializer
            .deserialize_batch_with_kafka_timestamps(&msg_refs, &kafka_ts)
            .expect("batch larger than 1024 rows should decode");

        assert_eq!(batch.num_rows(), row_count);
    }

    #[test]
    fn decode_json_batch_at_kafka_source_batch_size() {
        let deserializer = test_deserializer();
        let row_count = 4096;
        let messages: Vec<Vec<u8>> = (0..row_count)
            .map(|i| format!(r#"{{"id":{i},"value":"row-{i}"}}"#).into_bytes())
            .collect();
        let msg_refs: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
        let kafka_ts: Vec<u64> = (0..row_count).map(|i| i as u64).collect();

        let batch = deserializer
            .deserialize_batch_with_kafka_timestamps(&msg_refs, &kafka_ts)
            .expect("4096-row batch should decode without alignment error");

        assert_eq!(batch.num_rows(), row_count);
    }

    #[test]
    fn decode_ndjson_buffer_loops_past_default_batch_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let row_count = 3647;
        let mut buffer = Vec::new();
        for i in 0..row_count {
            buffer.extend_from_slice(format!(r#"{{"id":{i}}}"#).as_bytes());
            buffer.push(b'\n');
        }

        // Pre-fix behavior: one decode + one flush only yields 1024 rows.
        let mut decoder = ReaderBuilder::new(schema.clone())
            .with_strict_mode(false)
            .build_decoder()
            .unwrap();
        decoder.decode(&buffer).unwrap();
        let single_flush = decoder.flush().unwrap().unwrap();
        assert_eq!(single_flush.num_rows(), 1024);

        let batch = decode_ndjson_buffer(&schema, &buffer, row_count).expect("3647 rows");
        assert_eq!(batch.num_rows(), row_count);
    }
}
