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

use arrow_array::StringArray;
use arrow_schema::{DataType, Field, Schema};
use protocol::grpc::api::FsProgram;

use crate::sql::common::render_program_topology;

use super::DataSet;

#[derive(Clone, Debug)]
pub struct ShowCreateStreamingTableResult {
    table_name: String,
    status: String,
    pipeline_detail: String,
    program: FsProgram,
}

impl ShowCreateStreamingTableResult {
    pub fn new(
        table_name: String,
        status: String,
        pipeline_detail: String,
        program: FsProgram,
    ) -> Self {
        Self {
            table_name,
            status,
            pipeline_detail,
            program,
        }
    }
}

impl DataSet for ShowCreateStreamingTableResult {
    fn to_record_batch(&self) -> arrow_array::RecordBatch {
        let topology = render_program_topology(&self.program);

        let schema = Arc::new(Schema::new(vec![
            Field::new("Streaming Table", DataType::Utf8, false),
            Field::new("Status", DataType::Utf8, false),
            Field::new("Pipelines", DataType::Utf8, false),
            Field::new("Topology", DataType::Utf8, false),
        ]));

        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![self.table_name.as_str()])),
                Arc::new(StringArray::from(vec![self.status.as_str()])),
                Arc::new(StringArray::from(vec![self.pipeline_detail.as_str()])),
                Arc::new(StringArray::from(vec![topology.as_str()])),
            ],
        )
        .unwrap_or_else(|_| arrow_array::RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}
