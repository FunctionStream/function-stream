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

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::DataSet;
use crate::storage::task::FunctionInfo;

#[derive(Clone, Debug)]
pub struct ShowFunctionsResult {
    functions: Vec<FunctionInfo>,
}

impl ShowFunctionsResult {
    pub fn new(functions: Vec<FunctionInfo>) -> Self {
        Self { functions }
    }

    pub fn functions(&self) -> &[FunctionInfo] {
        &self.functions
    }
}

impl DataSet for ShowFunctionsResult {
    fn to_record_batch(&self) -> RecordBatch {
        let names: Vec<&str> = self.functions.iter().map(|f| f.name.as_str()).collect();
        let types: Vec<&str> = self
            .functions
            .iter()
            .map(|f| f.task_type.as_str())
            .collect();
        let statuses: Vec<&str> = self.functions.iter().map(|f| f.status.as_str()).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("task_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(types)),
                Arc::new(StringArray::from(statuses)),
            ],
        )
        .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}
