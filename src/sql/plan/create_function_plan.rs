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
use crate::sql::statement::{ConfigSource, FunctionSource};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateFunctionPlan {
    pub function_source: FunctionSource,
    pub config_source: Option<ConfigSource>,
    pub properties: HashMap<String, String>,
}

impl CreateFunctionPlan {
    pub fn new(
        function_source: FunctionSource,
        config_source: Option<ConfigSource>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            function_source,
            config_source,
            properties,
        }
    }
}

impl PlanNode for CreateFunctionPlan {
    fn accept(&self, visitor: &dyn PlanVisitor, context: &PlanVisitorContext) -> PlanVisitorResult {
        visitor.visit_create_function(self, context)
    }
}
