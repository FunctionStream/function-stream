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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimestampFormat {
    RFC3339,
    UnixMillis,
    UnixSeconds,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DecimalEncoding {
    String,
    Number,
    Bytes,
}

/// 数据容错策略
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BadDataPolicy {
    /// 遇到脏数据直接报错，导致算子 Panic 和重启
    Fail,
    /// 丢弃脏数据，并记录监控 Metrics
    Drop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonFormat {
    pub timestamp_format: TimestampFormat,
    pub decimal_encoding: DecimalEncoding,
    pub include_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Format {
    Json(JsonFormat),
    RawString,
    RawBytes,
}
