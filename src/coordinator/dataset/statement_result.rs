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

use arrow_array::RecordBatch;

use super::{DataSet, empty_record_batch};

/// RPC statement result: success/failure, message, optional RecordBatch; implements DataSet.
#[derive(Debug, Clone)]
pub struct StatementResult {
    pub success: bool,
    pub message: String,
    data: Option<RecordBatch>,
}

impl StatementResult {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: None,
        }
    }

    pub fn ok_with_data(message: impl Into<String>, data: RecordBatch) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }

    /// Convert to RecordBatch; returns the inner batch or an empty one (0 columns, 0 rows).
    pub fn to_record_batch(&self) -> RecordBatch {
        self.data
            .clone()
            .unwrap_or_else(empty_record_batch)
    }

    /// Consume self and return the inner RecordBatch; empty RecordBatch if no data.
    pub fn into_record_batch(self) -> RecordBatch {
        self.data.unwrap_or_else(empty_record_batch)
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    pub fn data(&self) -> Option<&RecordBatch> {
        self.data.as_ref()
    }
}

impl DataSet for StatementResult {
    fn to_record_batch(&self) -> RecordBatch {
        self.to_record_batch()
    }
}
