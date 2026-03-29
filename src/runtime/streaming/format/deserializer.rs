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


use anyhow::{anyhow, Result};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use arrow_json::reader::ReaderBuilder;
use arrow_schema::SchemaRef;
use std::sync::Arc;

use super::config::{BadDataPolicy, Format};

pub struct DataDeserializer {
    format: Format,
    schema: SchemaRef,
    bad_data_policy: BadDataPolicy,
}

impl DataDeserializer {
    pub fn new(format: Format, schema: SchemaRef, bad_data_policy: BadDataPolicy) -> Self {
        Self {
            format,
            schema,
            bad_data_policy,
        }
    }

    pub fn deserialize_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch> {
        match &self.format {
            Format::Json(_) => self.deserialize_json(messages),
            Format::RawString => self.deserialize_raw_string(messages),
            Format::RawBytes => self.deserialize_raw_bytes(messages),
        }
    }

    fn deserialize_json(&self, messages: &[&[u8]]) -> Result<RecordBatch> {
        let mut buffer = Vec::with_capacity(messages.len() * 256);
        for msg in messages {
            buffer.extend_from_slice(msg);
            buffer.push(b'\n');
        }

        let allow_bad_data = self.bad_data_policy == BadDataPolicy::Drop;
        let mut decoder = ReaderBuilder::new(self.schema.clone())
            .with_strict_mode(!allow_bad_data)
            .build_decoder()?;

        decoder.decode(&buffer)?;

        let batch = if allow_bad_data {
            let (batch, _mask, _, _errors) = decoder.flush_with_bad_data()?.unwrap();
            batch
        } else {
            decoder
                .flush()?
                .ok_or_else(|| anyhow!("JSON decoder returned no batch"))?
        };

        Ok(batch)
    }

    fn deserialize_raw_string(&self, messages: &[&[u8]]) -> Result<RecordBatch> {
        let mut builder = StringBuilder::with_capacity(messages.len(), messages.len() * 64);
        for msg in messages {
            builder.append_value(String::from_utf8_lossy(msg));
        }

        let array = Arc::new(builder.finish());
        RecordBatch::try_new(self.schema.clone(), vec![array])
            .map_err(|e| anyhow!("build RawString batch: {e}"))
    }

    fn deserialize_raw_bytes(&self, messages: &[&[u8]]) -> Result<RecordBatch> {
        use arrow_array::builder::BinaryBuilder;

        let mut builder = BinaryBuilder::with_capacity(messages.len(), messages.len() * 64);
        for msg in messages {
            builder.append_value(msg);
        }

        let array = Arc::new(builder.finish());
        RecordBatch::try_new(self.schema.clone(), vec![array])
            .map_err(|e| anyhow!("build RawBytes batch: {e}"))
    }
}
