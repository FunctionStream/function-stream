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

/// Source of function data (either file path or bytes)
#[derive(Debug, Clone)]
pub enum FunctionSource {
    Path(String),
    Bytes(Vec<u8>),
}

/// Source of config data (either file path or bytes)
#[derive(Debug, Clone)]
pub enum ConfigSource {
    Path(String),
    Bytes(Vec<u8>),
}


#[derive(Debug, Clone)]
pub struct CreateFunction {
    pub function_source: FunctionSource,
    pub config_source: Option<ConfigSource>,
    pub properties: HashMap<String, String>,
}

impl CreateFunction {
    
    pub const PROP_FUNCTION_PATH: &'static str = "function_path";

    pub const PROP_CONFIG_PATH: &'static str = "config_path";

    pub fn from_bytes(
        function_bytes: Vec<u8>,
        config_bytes: Option<Vec<u8>>,
    ) -> Self {
        Self {
            function_source: FunctionSource::Bytes(function_bytes),
            config_source: config_bytes.map(ConfigSource::Bytes),
            properties: HashMap::new(),
        }
    }

    pub fn from_properties(properties: HashMap<String, String>) -> Result<Self, String> {
        let function_source = Self::parse_function_path(&properties)?;
        let config_source = Self::parse_config_path(&properties);
        let extra_props = Self::extract_extra_properties(&properties);
        
        Ok(Self {
            function_source,
            config_source,
            properties: extra_props,
        })
    }

    /// Parse function path from properties (SQL only, Path mode)
    fn parse_function_path(properties: &HashMap<String, String>) -> Result<FunctionSource, String> {
        // SQL only supports function_path (file path), not function (bytes)
        if let Some(path) = Self::get_property_ci(properties, Self::PROP_FUNCTION_PATH) {
            return Ok(FunctionSource::Path(path));
        }

        Err(format!(
            "Missing required property '{}' (case-insensitive). SQL only supports path mode, not bytes mode.",
            Self::PROP_FUNCTION_PATH
        ))
    }

    /// Parse config path from properties (SQL only, Path mode)
    fn parse_config_path(properties: &HashMap<String, String>) -> Option<ConfigSource> {
        // SQL only supports config_path (file path), not config (bytes)
        if let Some(path) = Self::get_property_ci(properties, Self::PROP_CONFIG_PATH) {
            return Some(ConfigSource::Path(path));
        }

        None
    }

    /// Extract extra properties (excluding function/config related properties)
    fn extract_extra_properties(properties: &HashMap<String, String>) -> HashMap<String, String> {
        let mut extra_props = properties.clone();
        // Remove function_path and config_path properties (case-insensitive)
        let keys_to_remove: Vec<String> = extra_props
            .keys()
            .filter(|k| {
                let k_lower = k.to_lowercase();
                k_lower == Self::PROP_FUNCTION_PATH
                    || k_lower == Self::PROP_CONFIG_PATH
            })
            .cloned()
            .collect();
        for key in keys_to_remove {
            extra_props.remove(&key);
        }
        extra_props
    }

    /// Find property value by case-insensitive key
    fn get_property_ci(properties: &HashMap<String, String>, key: &str) -> Option<String> {
        let key_lower = key.to_lowercase();
        for (k, v) in properties {
            if k.to_lowercase() == key_lower {
                return Some(v.clone());
            }
        }
        None
    }

    /// Get function source
    pub fn get_function_source(&self) -> &FunctionSource {
        &self.function_source
    }

    /// Get config source
    pub fn get_config_source(&self) -> Option<&ConfigSource> {
        self.config_source.as_ref()
    }

    /// Get extra properties
    pub fn get_extra_properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}
impl Statement for CreateFunction {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult {
        visitor.visit_create_function(self, context)
    }
}
