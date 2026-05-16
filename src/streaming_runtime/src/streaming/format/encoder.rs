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

use std::io::Cursor;

use anyhow::{Context, Result, bail};
use apache_avro::types::Value as AvroValue;
use apache_avro::{Codec as AvroCodec, Schema as AvroSchema, Writer as AvroWriter};
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::json::LineDelimitedWriter;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::writer::FileWriter as ArrowIpcFileWriter;
use arrow_schema::{DataType, Field, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Pure in-memory format encoder for sink payload generation.
/// It only converts `RecordBatch` into bytes and does not perform any I/O.
pub struct FormatEncoder;

impl FormatEncoder {
    /// Encode batches into CSV bytes.
    pub fn encode_csv(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut out);
        for batch in batches {
            writer
                .write(batch)
                .context("failed to encode record batch to CSV")?;
        }
        drop(writer);
        Ok(out)
    }

    /// Encode batches into Parquet bytes.
    pub fn encode_parquet(batches: &[RecordBatch], compression: Compression) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))
            .context("failed to init parquet writer")?;

        for batch in batches {
            writer
                .write(batch)
                .context("failed to encode record batch to parquet")?;
        }
        writer.close().context("failed to finalize parquet")?;
        Ok(cursor.into_inner())
    }

    /// Encode batches into NDJSON (JSON Lines) bytes.
    pub fn encode_jsonl(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut writer = LineDelimitedWriter::new(&mut out);
        for batch in batches {
            writer
                .write(batch)
                .context("failed to encode record batch to JSONL")?;
        }
        writer.finish().context("failed to finalize JSONL stream")?;
        Ok(out)
    }

    pub fn encode_avro(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema_json = build_avro_schema_json(&batches[0])?;
        let avro_schema =
            AvroSchema::parse_str(&schema_json).context("failed to parse generated avro schema")?;
        let mut writer = AvroWriter::with_codec(&avro_schema, Vec::new(), AvroCodec::Null);

        for batch in batches {
            let accessors = build_column_accessors(batch)?;
            let schema = batch.schema();
            let fields = schema.fields();
            let col_names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();

            for row_idx in 0..batch.num_rows() {
                let mut row_records = Vec::with_capacity(accessors.len());
                for (col_idx, accessor) in accessors.iter().enumerate() {
                    let nullable = fields[col_idx].is_nullable();
                    let val = accessor.avro_value_at(row_idx, nullable)?;
                    row_records.push((col_names[col_idx].clone(), val));
                }
                writer
                    .append(AvroValue::Record(row_records))
                    .map_err(|e| anyhow::anyhow!("failed to append row into avro writer: {e}"))?;
            }
        }

        writer.flush().context("failed to flush avro writer")?;
        writer
            .into_inner()
            .context("failed to finalize avro container bytes")
    }

    pub fn encode_orc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let mut out = Cursor::new(Vec::new());
        let mut writer = ArrowIpcFileWriter::try_new(&mut out, &schema)
            .context("failed to init ORC-compatible file writer")?;

        for batch in batches {
            writer
                .write(batch)
                .context("failed to encode record batch into ORC-compatible payload")?;
        }

        writer
            .finish()
            .context("failed to finalize ORC-compatible payload")?;
        Ok(out.into_inner())
    }
}

fn build_avro_schema_json(batch: &RecordBatch) -> Result<String> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let avro_type = avro_type_for_field(field)?;
            let field_type = if field.is_nullable() {
                JsonValue::Array(vec![JsonValue::String("null".to_string()), avro_type])
            } else {
                avro_type
            };
            Ok(JsonValue::Object(JsonMap::from_iter([
                (
                    "name".to_string(),
                    JsonValue::String(field.name().to_string()),
                ),
                ("type".to_string(), field_type),
                ("default".to_string(), JsonValue::Null),
            ])))
        })
        .collect::<Result<Vec<_>>>()?;

    let schema = JsonValue::Object(JsonMap::from_iter([
        ("type".to_string(), JsonValue::String("record".to_string())),
        (
            "name".to_string(),
            JsonValue::String("FunctionStreamRecord".to_string()),
        ),
        ("fields".to_string(), JsonValue::Array(fields)),
    ]));

    serde_json::to_string(&schema).context("failed to serialize avro schema json")
}

fn avro_type_for_field(field: &Field) -> Result<JsonValue> {
    let ty = match field.data_type() {
        DataType::Boolean => JsonValue::String("boolean".to_string()),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => JsonValue::String("int".to_string()),
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Int64
        | DataType::UInt64 => JsonValue::String("long".to_string()),
        DataType::Float32 => JsonValue::String("float".to_string()),
        DataType::Float64 => JsonValue::String("double".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => JsonValue::String("string".to_string()),
        DataType::Binary | DataType::LargeBinary => JsonValue::String("bytes".to_string()),
        DataType::Date32 => JsonValue::Object(JsonMap::from_iter([
            ("type".to_string(), JsonValue::String("int".to_string())),
            (
                "logicalType".to_string(),
                JsonValue::String("date".to_string()),
            ),
        ])),
        DataType::Timestamp(unit, _) => {
            let logical = match unit {
                TimeUnit::Second | TimeUnit::Millisecond => "timestamp-millis",
                TimeUnit::Microsecond | TimeUnit::Nanosecond => "timestamp-micros",
            };
            JsonValue::Object(JsonMap::from_iter([
                ("type".to_string(), JsonValue::String("long".to_string())),
                (
                    "logicalType".to_string(),
                    JsonValue::String(logical.to_string()),
                ),
            ]))
        }
        other => bail!("unsupported data type for avro encoding: {other:?}"),
    };
    Ok(ty)
}

/// Downcasts each column once per batch; row iteration only matches on this enum.
enum ColumnAccessor<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    Date32(&'a Int32Array),
    TimestampSec(&'a TimestampSecondArray),
    TimestampMs(&'a TimestampMillisecondArray),
    TimestampUs(&'a TimestampMicrosecondArray),
    TimestampNs(&'a TimestampNanosecondArray),
}

impl<'a> ColumnAccessor<'a> {
    fn avro_value_at(&self, row: usize, nullable: bool) -> Result<AvroValue> {
        let is_null = match self {
            Self::Boolean(a) => a.is_null(row),
            Self::Int8(a) => a.is_null(row),
            Self::Int16(a) => a.is_null(row),
            Self::Int32(a) => a.is_null(row),
            Self::Int64(a) => a.is_null(row),
            Self::UInt8(a) => a.is_null(row),
            Self::UInt16(a) => a.is_null(row),
            Self::UInt32(a) => a.is_null(row),
            Self::UInt64(a) => a.is_null(row),
            Self::Float32(a) => a.is_null(row),
            Self::Float64(a) => a.is_null(row),
            Self::Utf8(a) => a.is_null(row),
            Self::LargeUtf8(a) => a.is_null(row),
            Self::Binary(a) => a.is_null(row),
            Self::LargeBinary(a) => a.is_null(row),
            Self::Date32(a) => a.is_null(row),
            Self::TimestampSec(a) => a.is_null(row),
            Self::TimestampMs(a) => a.is_null(row),
            Self::TimestampUs(a) => a.is_null(row),
            Self::TimestampNs(a) => a.is_null(row),
        };

        if is_null {
            if !nullable {
                bail!("null value in non-nullable avro field at row {row}");
            }
            return Ok(AvroValue::Union(0, Box::new(AvroValue::Null)));
        }

        let raw = match self {
            Self::Boolean(a) => AvroValue::Boolean(a.value(row)),
            Self::Int8(a) => AvroValue::Int(i32::from(a.value(row))),
            Self::Int16(a) => AvroValue::Int(i32::from(a.value(row))),
            Self::Int32(a) => AvroValue::Int(a.value(row)),
            Self::Int64(a) => AvroValue::Long(a.value(row)),
            Self::UInt8(a) => AvroValue::Int(i32::from(a.value(row))),
            Self::UInt16(a) => AvroValue::Int(i32::from(a.value(row))),
            Self::UInt32(a) => AvroValue::Long(i64::from(a.value(row))),
            Self::UInt64(a) => {
                let v = a.value(row);
                AvroValue::Long(i64::try_from(v).with_context(|| {
                    format!("UInt64 value {v} does not fit Avro long for row {row}")
                })?)
            }
            Self::Float32(a) => {
                let v = a.value(row);
                if !v.is_finite() {
                    bail!("non-finite f32 at row {row}: {v}");
                }
                AvroValue::Float(v)
            }
            Self::Float64(a) => {
                let v = a.value(row);
                if !v.is_finite() {
                    bail!("non-finite f64 at row {row}: {v}");
                }
                AvroValue::Double(v)
            }
            Self::Utf8(a) => AvroValue::String(a.value(row).to_string()),
            Self::LargeUtf8(a) => AvroValue::String(a.value(row).to_string()),
            Self::Binary(a) => AvroValue::Bytes(a.value(row).to_vec()),
            Self::LargeBinary(a) => AvroValue::Bytes(a.value(row).to_vec()),
            Self::Date32(a) => AvroValue::Int(a.value(row)),
            Self::TimestampSec(a) => AvroValue::Long(a.value(row).saturating_mul(1000)),
            Self::TimestampMs(a) => AvroValue::Long(a.value(row)),
            Self::TimestampUs(a) => AvroValue::Long(a.value(row)),
            Self::TimestampNs(a) => AvroValue::Long(a.value(row) / 1000),
        };

        Ok(if nullable {
            AvroValue::Union(1, Box::new(raw))
        } else {
            raw
        })
    }
}

fn build_column_accessors(batch: &RecordBatch) -> Result<Vec<ColumnAccessor<'_>>> {
    let mut accessors = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let accessor = match col.data_type() {
            DataType::Boolean => ColumnAccessor::Boolean(
                col.as_any()
                    .downcast_ref()
                    .context("expected BooleanArray")?,
            ),
            DataType::Int8 => {
                ColumnAccessor::Int8(col.as_any().downcast_ref().context("expected Int8Array")?)
            }
            DataType::Int16 => {
                ColumnAccessor::Int16(col.as_any().downcast_ref().context("expected Int16Array")?)
            }
            DataType::Int32 => {
                ColumnAccessor::Int32(col.as_any().downcast_ref().context("expected Int32Array")?)
            }
            DataType::Int64 => {
                ColumnAccessor::Int64(col.as_any().downcast_ref().context("expected Int64Array")?)
            }
            DataType::UInt8 => {
                ColumnAccessor::UInt8(col.as_any().downcast_ref().context("expected UInt8Array")?)
            }
            DataType::UInt16 => ColumnAccessor::UInt16(
                col.as_any()
                    .downcast_ref()
                    .context("expected UInt16Array")?,
            ),
            DataType::UInt32 => ColumnAccessor::UInt32(
                col.as_any()
                    .downcast_ref()
                    .context("expected UInt32Array")?,
            ),
            DataType::UInt64 => ColumnAccessor::UInt64(
                col.as_any()
                    .downcast_ref()
                    .context("expected UInt64Array")?,
            ),
            DataType::Float32 => ColumnAccessor::Float32(
                col.as_any()
                    .downcast_ref()
                    .context("expected Float32Array")?,
            ),
            DataType::Float64 => ColumnAccessor::Float64(
                col.as_any()
                    .downcast_ref()
                    .context("expected Float64Array")?,
            ),
            DataType::Utf8 => ColumnAccessor::Utf8(
                col.as_any()
                    .downcast_ref()
                    .context("expected StringArray")?,
            ),
            DataType::LargeUtf8 => ColumnAccessor::LargeUtf8(
                col.as_any()
                    .downcast_ref()
                    .context("expected LargeStringArray")?,
            ),
            DataType::Binary => ColumnAccessor::Binary(
                col.as_any()
                    .downcast_ref()
                    .context("expected BinaryArray")?,
            ),
            DataType::LargeBinary => ColumnAccessor::LargeBinary(
                col.as_any()
                    .downcast_ref()
                    .context("expected LargeBinaryArray")?,
            ),
            DataType::Date32 => {
                ColumnAccessor::Date32(col.as_any().downcast_ref().context("expected Int32Array")?)
            }
            DataType::Timestamp(TimeUnit::Second, _) => ColumnAccessor::TimestampSec(
                col.as_any()
                    .downcast_ref()
                    .context("expected TimestampSecondArray")?,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => ColumnAccessor::TimestampMs(
                col.as_any()
                    .downcast_ref()
                    .context("expected TimestampMillisecondArray")?,
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => ColumnAccessor::TimestampUs(
                col.as_any()
                    .downcast_ref()
                    .context("expected TimestampMicrosecondArray")?,
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => ColumnAccessor::TimestampNs(
                col.as_any()
                    .downcast_ref()
                    .context("expected TimestampNanosecondArray")?,
            ),
            other => bail!("unsupported data type for avro column accessor: {other:?}"),
        };
        accessors.push(accessor);
    }
    Ok(accessors)
}
