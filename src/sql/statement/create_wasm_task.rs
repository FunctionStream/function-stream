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
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateWasmTask {
    pub name: String,
    pub properties: HashMap<String, String>,
}

impl CreateWasmTask {
    pub const PROP_WASM_PATH: &'static str = "wasm-path";
    pub const PROP_WASM_PATH_ALT: &'static str = "wasm_path";
    pub const PROP_CONFIG_PATH: &'static str = "config-path";
    pub const PROP_CONFIG_PATH_ALT: &'static str = "config_path";

    pub fn new(name: String, properties: HashMap<String, String>) -> Self {
        Self { name, properties }
    }

    pub fn get_wasm_path(&self) -> Result<String, String> {
        self.properties
            .get(Self::PROP_WASM_PATH)
            .or_else(|| self.properties.get(Self::PROP_WASM_PATH_ALT))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Missing required property '{}' or '{}'",
                    Self::PROP_WASM_PATH,
                    Self::PROP_WASM_PATH_ALT
                )
            })
    }

    pub fn get_config_path(&self) -> Option<String> {
        self.properties
            .get(Self::PROP_CONFIG_PATH)
            .or_else(|| self.properties.get(Self::PROP_CONFIG_PATH_ALT))
            .cloned()
    }

    pub fn get_extra_properties(&self) -> HashMap<String, String> {
        let mut extra_props = self.properties.clone();
        extra_props.remove(Self::PROP_WASM_PATH);
        extra_props.remove(Self::PROP_WASM_PATH_ALT);
        extra_props.remove(Self::PROP_CONFIG_PATH);
        extra_props.remove(Self::PROP_CONFIG_PATH_ALT);
        extra_props
    }
}
impl Statement for CreateWasmTask {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_create_wasm_task(self, context)
    }
}
