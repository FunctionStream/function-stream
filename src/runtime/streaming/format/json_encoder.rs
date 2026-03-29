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

//! 极致优化的 Arrow JSON 编码器。
//!
//! 解决 Arrow 原生 JSON 导出时不兼容 Kafka / 时间戳 / Decimal 的痛点。

use arrow_array::{
    Array, Decimal128Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_json::writer::NullableEncoder;
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, DataType, FieldRef, TimeUnit};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;

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
        let encoder: Box<dyn Encoder> = match (
            &self.decimal_encoding,
            &self.timestamp_format,
            array.data_type(),
        ) {
            // ── Timestamp → Unix 毫秒 ──
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .clone();
                Box::new(UnixMillisEncoder::Nanos(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .clone();
                Box::new(UnixMillisEncoder::Micros(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .clone();
                Box::new(UnixMillisEncoder::Millis(arr))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Second, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .clone();
                Box::new(UnixMillisEncoder::Seconds(arr))
            }

            // ── Decimal128 → String / Bytes ──
            (DecimalEncoding::String, _, DataType::Decimal128(_, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .clone();
                Box::new(DecimalEncoder::StringEncoder(arr))
            }
            (DecimalEncoding::Bytes, _, DataType::Decimal128(_, _)) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .clone();
                Box::new(DecimalEncoder::BytesEncoder(arr))
            }

            // ── Binary → Base64 ──
            (_, _, DataType::Binary) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap()
                    .clone();
                Box::new(BinaryEncoder(arr))
            }

            // 其他类型：降级使用 Arrow 原生 encoder
            _ => return Ok(None),
        };

        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

// ---------------------------------------------------------------------------
// UnixMillisEncoder — 各精度 Timestamp → i64 毫秒
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
        out.extend_from_slice(millis.to_string().as_bytes());
    }
}

// ---------------------------------------------------------------------------
// DecimalEncoder — Decimal128 → JSON 字符串 / Base64 Bytes
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
                out.push(b'"');
                out.extend_from_slice(
                    BASE64_STANDARD
                        .encode(arr.value(idx).to_be_bytes())
                        .as_bytes(),
                );
                out.push(b'"');
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BinaryEncoder — Binary → Base64 字符串
// ---------------------------------------------------------------------------

struct BinaryEncoder(arrow_array::BinaryArray);

impl Encoder for BinaryEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        out.extend_from_slice(BASE64_STANDARD.encode(self.0.value(idx)).as_bytes());
        out.push(b'"');
    }
}
