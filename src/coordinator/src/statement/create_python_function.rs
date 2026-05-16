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

/// Module information for Python function execution
#[derive(Debug, Clone)]
pub struct PythonModule {
    pub name: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CreatePythonFunction {
    pub class_name: String,
    pub modules: Vec<PythonModule>,
    pub config_content: String,
}

impl CreatePythonFunction {
    pub fn new(class_name: String, modules: Vec<PythonModule>, config_content: String) -> Self {
        Self {
            class_name,
            modules,
            config_content,
        }
    }

    pub fn get_class_name(&self) -> &str {
        &self.class_name
    }

    pub fn get_modules(&self) -> &[PythonModule] {
        &self.modules
    }

    pub fn get_config_content(&self) -> &str {
        &self.config_content
    }
}

impl Statement for CreatePythonFunction {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_create_python_function(self, context)
    }
}
