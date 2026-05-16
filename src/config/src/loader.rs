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

use serde_yaml::Value;
use std::fs;
use std::path::Path;

/// Read configuration from YAML file
pub fn read_yaml_file<P: AsRef<Path>>(path: P) -> Result<Value, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let value: Value = serde_yaml::from_str(&content)?;
    Ok(value)
}

/// Load global configuration from file
pub fn load_global_config<P: AsRef<Path>>(
    path: P,
) -> Result<crate::config::GlobalConfig, Box<dyn std::error::Error>> {
    let value = read_yaml_file(path)?;
    crate::config::GlobalConfig::from_yaml_value(value)
}
