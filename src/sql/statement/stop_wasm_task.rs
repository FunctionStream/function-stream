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

#[derive(Debug, Clone)]
pub struct StopWasmTask {
    pub name: String,
}

impl StopWasmTask {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Statement for StopWasmTask {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_stop_wasm_task(self, context)
    }
}
