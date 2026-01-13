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

/// Read configuration from YAML string
pub fn read_yaml_str(content: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let value: Value = serde_yaml::from_str(content)?;
    Ok(value)
}

/// Read YAML configuration from byte array
pub fn read_yaml_bytes(bytes: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    use std::io::Cursor;
    let cursor = Cursor::new(bytes);
    let value: Value = serde_yaml::from_reader(cursor)?;
    Ok(value)
}

/// Read YAML configuration from any type implementing Read trait
pub fn read_yaml_reader<R: std::io::Read>(reader: R) -> Result<Value, Box<dyn std::error::Error>> {
    let value: Value = serde_yaml::from_reader(reader)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
name: test-app
version: 1.0.0
debug: true
items:
  - item1
  - item2
  - name: nested_item
    value: 42
    metadata:
      created: "2024-01-01"
      tags:
        - important
        - test
config:
  host: localhost
  port: 3000
  database:
    primary:
      host: db1.example.com
      port: 5432
      credentials:
        username: admin
        password: secret
    replica:
      - host: db2.example.com
        port: 5433
      - host: db3.example.com
        port: 5434
"#;

        let result = read_yaml_str(yaml).unwrap();

        // Check basic fields
        assert_eq!(result["name"], "test-app");
        assert_eq!(result["version"], "1.0.0");
        assert_eq!(result["debug"], true);

        // Check simple array elements
        assert_eq!(result["items"][0], "item1");
        assert_eq!(result["items"][1], "item2");

        // Check nested object array elements
        assert_eq!(result["items"][2]["name"], "nested_item");
        assert_eq!(result["items"][2]["value"], 42);
        assert_eq!(result["items"][2]["metadata"]["created"], "2024-01-01");
        assert_eq!(result["items"][2]["metadata"]["tags"][0], "important");
        assert_eq!(result["items"][2]["metadata"]["tags"][1], "test");

        // Check nested objects
        assert_eq!(result["config"]["host"], "localhost");
        assert_eq!(result["config"]["port"], 3000);

        // Check deep nesting (3 levels)
        assert_eq!(
            result["config"]["database"]["primary"]["host"],
            "db1.example.com"
        );
        assert_eq!(result["config"]["database"]["primary"]["port"], 5432);
        assert_eq!(
            result["config"]["database"]["primary"]["credentials"]["username"],
            "admin"
        );
        assert_eq!(
            result["config"]["database"]["primary"]["credentials"]["password"],
            "secret"
        );

        // Check array nesting
        assert_eq!(
            result["config"]["database"]["replica"][0]["host"],
            "db2.example.com"
        );
        assert_eq!(result["config"]["database"]["replica"][0]["port"], 5433);
        assert_eq!(
            result["config"]["database"]["replica"][1]["host"],
            "db3.example.com"
        );
        assert_eq!(result["config"]["database"]["replica"][1]["port"], 5434);
    }

    #[test]
    fn test_yaml_bytes() {
        let yaml_bytes = br#"
byte_test:
  type: bytes
values:
  - 1
  - 2
  - 3
metadata:
  source: binary
  size: 1024
"#;

        let result = read_yaml_bytes(yaml_bytes).unwrap();

        assert_eq!(result["byte_test"]["type"], "bytes");
        assert_eq!(result["values"][0], 1);
        assert_eq!(result["values"][1], 2);
        assert_eq!(result["values"][2], 3);
        assert_eq!(result["metadata"]["source"], "binary");
        assert_eq!(result["metadata"]["size"], 1024);
    }

    #[test]
    fn test_yaml_reader() {
        let yaml_data = r#"
reader_test:
  method: cursor
  data:
    key1: value1
    key2: value2
status: active
"#;

        let cursor = Cursor::new(yaml_data.as_bytes());
        let result = read_yaml_reader(cursor).unwrap();

        assert_eq!(result["reader_test"]["method"], "cursor");
        assert_eq!(result["reader_test"]["data"]["key1"], "value1");
        assert_eq!(result["reader_test"]["data"]["key2"], "value2");
        assert_eq!(result["status"], "active");
    }
}

/// Load global configuration from file
pub fn load_global_config<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<crate::config::GlobalConfig, Box<dyn std::error::Error>> {
    let value = read_yaml_file(path)?;
    crate::config::GlobalConfig::from_yaml_value(value)
}
/// Load global configuration from string
pub fn load_global_config_from_str(
    content: &str,
) -> Result<crate::config::GlobalConfig, Box<dyn std::error::Error>> {
    let value = read_yaml_str(content)?;
    crate::config::GlobalConfig::from_yaml_value(value)
}
