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

use super::{Statement, StatementVisitor, StatementVisitorContext, StatementVisitorResult};

/// `SHOW STREAMING TABLES` — lists all active streaming jobs managed by `JobManager`.
#[derive(Debug, Clone, Default)]
pub struct ShowStreamingTables;

impl ShowStreamingTables {
    pub fn new() -> Self {
        Self
    }
}

impl Statement for ShowStreamingTables {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_show_streaming_tables(self, context)
    }
}
