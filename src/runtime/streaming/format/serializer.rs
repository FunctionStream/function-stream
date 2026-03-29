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

//! 数据序列化器：将内存 [`RecordBatch`] 转换为二进制消息流，供 Sink 连接器发送。

use anyhow::{anyhow, Result};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_json::writer::make_encoder;
use arrow_json::EncoderOptions;
use arrow_schema::{DataType, Field, SchemaRef};
use std::sync::Arc;

use super::config::{Format, JsonFormat};
use super::json_encoder::CustomEncoderFactory;

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
            .filter(|(_, f)| !f.name().starts_with('_'))
            .map(|(i, _)| i)
            .collect();

        Self {
            format,
            projection_indices,
        }
    }

    pub fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let projected_batch = batch.project(&self.projection_indices)?;

        match &self.format {
            Format::Json(config) => self.serialize_json(config, &projected_batch),
            Format::RawString => self.serialize_raw_string(&projected_batch),
            Format::RawBytes => self.serialize_raw_bytes(&projected_batch),
        }
    }

    fn serialize_json(&self, config: &JsonFormat, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let array = StructArray::from(batch.clone());
        let field = Arc::new(Field::new_struct(
            "",
            batch.schema().fields().clone(),
            false,
        ));

        let options = EncoderOptions::default()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(CustomEncoderFactory {
                timestamp_format: config.timestamp_format.clone(),
                decimal_encoding: config.decimal_encoding.clone(),
            }));

        let mut encoder = make_encoder(&field, &array, &options)?;
        let mut results = Vec::with_capacity(batch.num_rows());

        for idx in 0..array.len() {
            let mut buffer = Vec::with_capacity(128);
            encoder.encode(idx, &mut buffer);
            if !buffer.is_empty() {
                results.push(buffer);
            }
        }
        Ok(results)
    }

    fn serialize_raw_string(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let value_idx = batch
            .schema()
            .index_of("value")
            .map_err(|_| anyhow!("RawString format requires a 'value' column"))?;

        if *batch.schema().field(value_idx).data_type() != DataType::Utf8 {
            return Err(anyhow!("RawString 'value' column must be Utf8"));
        }

        let string_array = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        let values: Vec<Vec<u8>> = (0..string_array.len())
            .map(|i| {
                if string_array.is_null(i) {
                    vec![]
                } else {
                    string_array.value(i).as_bytes().to_vec()
                }
            })
            .collect();

        Ok(values)
    }

    fn serialize_raw_bytes(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let value_idx = batch
            .schema()
            .index_of("value")
            .map_err(|_| anyhow!("RawBytes format requires a 'value' column"))?;

        if *batch.schema().field(value_idx).data_type() != DataType::Binary {
            return Err(anyhow!("RawBytes 'value' column must be Binary"));
        }

        let binary_array = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        let values: Vec<Vec<u8>> = (0..binary_array.len())
            .map(|i| {
                if binary_array.is_null(i) {
                    vec![]
                } else {
                    binary_array.value(i).to_vec()
                }
            })
            .collect();

        Ok(values)
    }
}
