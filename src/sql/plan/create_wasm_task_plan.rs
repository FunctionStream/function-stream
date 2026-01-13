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

use super::{PlanNode, PlanVisitor, PlanVisitorContext, PlanVisitorResult};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateWasmTaskPlan {
    pub name: String,
    pub wasm_path: String,
    pub config_path: Option<String>,
    pub properties: HashMap<String, String>,
}

impl CreateWasmTaskPlan {
    pub fn new(
        name: String,
        wasm_path: String,
        config_path: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            wasm_path,
            config_path,
            properties,
        }
    }
}

impl PlanNode for CreateWasmTaskPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_create_wasm_task(self, context)
    }
}
