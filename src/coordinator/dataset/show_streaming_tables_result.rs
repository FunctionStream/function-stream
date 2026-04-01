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

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::DataSet;
use crate::runtime::streaming::job::StreamingJobSummary;

#[derive(Clone, Debug)]
pub struct ShowStreamingTablesResult {
    jobs: Vec<StreamingJobSummary>,
}

impl ShowStreamingTablesResult {
    pub fn new(jobs: Vec<StreamingJobSummary>) -> Self {
        Self { jobs }
    }
}

impl DataSet for ShowStreamingTablesResult {
    fn to_record_batch(&self) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("job_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("pipeline_count", DataType::Int32, false),
            Field::new("uptime", DataType::Utf8, false),
        ]));

        let job_ids: Vec<&str> = self.jobs.iter().map(|j| j.job_id.as_str()).collect();
        let statuses: Vec<&str> = self.jobs.iter().map(|j| j.status.as_str()).collect();
        let pipeline_counts: Vec<i32> = self.jobs.iter().map(|j| j.pipeline_count).collect();
        let uptimes: Vec<String> = self.jobs.iter().map(|j| format_duration(j.uptime_secs)).collect();
        let uptime_refs: Vec<&str> = uptimes.iter().map(|s| s.as_str()).collect();

        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(job_ids)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(Int32Array::from(pipeline_counts)),
                Arc::new(StringArray::from(uptime_refs)),
            ],
        )
        .unwrap_or_else(|_| arrow_array::RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

fn format_duration(total_secs: u64) -> String {
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if days > 0 {
        format!("{days}d {hours}h {mins}m {secs}s")
    } else if hours > 0 {
        format!("{hours}h {mins}m {secs}s")
    } else if mins > 0 {
        format!("{mins}m {secs}s")
    } else {
        format!("{secs}s")
    }
}
