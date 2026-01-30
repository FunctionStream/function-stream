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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

/// Create an empty RecordBatch (0 columns, 0 rows).
pub fn empty_record_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

/// DataSet interface: conversion to Arrow RecordBatch.
pub trait DataSet: Send + Sync {
    /// Convert to RecordBatch.
    fn to_record_batch(&self) -> RecordBatch;
}

impl DataSet for RecordBatch {
    fn to_record_batch(&self) -> RecordBatch {
        self.clone()
    }
}
