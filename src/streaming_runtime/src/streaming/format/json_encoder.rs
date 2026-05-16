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

use arrow_array::{
    Array, BinaryArray, Decimal128Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_json::writer::NullableEncoder;
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, DataType, FieldRef, TimeUnit};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use std::io::Write;

use super::config::{DecimalEncoding, TimestampFormat};

#[derive(Debug)]
pub struct CustomEncoderFactory {
    pub timestamp_format: TimestampFormat,
    pub decimal_encoding: DecimalEncoding,
}

impl EncoderFactory for CustomEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let downcast_err = |expected: &str| {
            ArrowError::CastError(format!(
                "Physical array type mismatch: expected {} for logical type {:?}",
                expected,
                array.data_type()
            ))
        };

        let encoder: Box<dyn Encoder> = match (
            &self.decimal_encoding,
            &self.timestamp_format,
            array.data_type(),
        ) {
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| downcast_err("TimestampNanosecondArray"))?
                    .clone();
                Box::new(UnixMillisEncoder::Nanos(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| downcast_err("TimestampMicrosecondArray"))?
                    .clone();
                Box::new(UnixMillisEncoder::Micros(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| downcast_err("TimestampMillisecondArray"))?
                    .clone();
                Box::new(UnixMillisEncoder::Millis(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Second, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| downcast_err("TimestampSecondArray"))?
                    .clone();
                Box::new(UnixMillisEncoder::Seconds(arr))
            }

            (DecimalEncoding::String, _, DataType::Decimal128(_, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| downcast_err("Decimal128Array"))?
                    .clone();
                Box::new(DecimalEncoder::StringEncoder(arr))
            }
            (DecimalEncoding::Bytes, _, DataType::Decimal128(_, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| downcast_err("Decimal128Array"))?
                    .clone();
                Box::new(DecimalEncoder::BytesEncoder(arr))
            }

            (_, _, DataType::Binary) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| downcast_err("BinaryArray"))?
                    .clone();
                Box::new(BinaryEncoder(arr))
            }

            _ => return Ok(None),
        };

        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

// ---------------------------------------------------------------------------
// Timestamp Encoders
// ---------------------------------------------------------------------------

enum UnixMillisEncoder {
    Nanos(TimestampNanosecondArray),
    Micros(TimestampMicrosecondArray),
    Millis(TimestampMillisecondArray),
    Seconds(TimestampSecondArray),
}

impl Encoder for UnixMillisEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let millis = match self {
            Self::Nanos(arr) => arr.value(idx) / 1_000_000,
            Self::Micros(arr) => arr.value(idx) / 1_000,
            Self::Millis(arr) => arr.value(idx),
            Self::Seconds(arr) => arr.value(idx) * 1_000,
        };

        write!(out, "{millis}").expect("Writing integer to Vec<u8> buffer should never fail");
    }
}

// ---------------------------------------------------------------------------
// Decimal Encoders
// ---------------------------------------------------------------------------

enum DecimalEncoder {
    StringEncoder(Decimal128Array),
    BytesEncoder(Decimal128Array),
}

impl Encoder for DecimalEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self {
            Self::StringEncoder(arr) => {
                out.push(b'"');
                out.extend_from_slice(arr.value_as_string(idx).as_bytes());
                out.push(b'"');
            }
            Self::BytesEncoder(arr) => {
                let bytes = arr.value(idx).to_be_bytes();
                let mut stack_buf = [0u8; 24];
                BASE64_STANDARD
                    .encode_slice(bytes, &mut stack_buf)
                    .expect("Base64 encode_slice size mismatch");

                out.push(b'"');
                out.extend_from_slice(&stack_buf);
                out.push(b'"');
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binary Encoder
// ---------------------------------------------------------------------------

struct BinaryEncoder(BinaryArray);

impl Encoder for BinaryEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let bytes = self.0.value(idx);

        let b64_len = bytes.len().saturating_add(2) / 3 * 4;

        out.push(b'"');

        let start_idx = out.len();
        out.resize(start_idx + b64_len, 0);

        BASE64_STANDARD
            .encode_slice(bytes, &mut out[start_idx..])
            .expect("Base64 encode_slice buffer capacity should match exactly");

        out.push(b'"');
    }
}
