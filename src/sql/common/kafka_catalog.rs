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

//!
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KafkaTable {
    pub topic: String,
    #[serde(flatten)]
    pub kind: TableType,
    #[serde(default)]
    pub client_configs: HashMap<String, String>,
    pub value_subject: Option<String>,
}

impl KafkaTable {
    pub fn subject(&self) -> String {
        self.value_subject
            .clone()
            .unwrap_or_else(|| format!("{}-value", self.topic))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableType {
    Source {
        offset: KafkaTableSourceOffset,
        read_mode: Option<ReadMode>,
        group_id: Option<String>,
        group_id_prefix: Option<String>,
    },
    Sink {
        commit_mode: SinkCommitMode,
        key_field: Option<String>,
        timestamp_field: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KafkaTableSourceOffset {
    Latest,
    Earliest,
    #[default]
    Group,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkCommitMode {
    #[default]
    AtLeastOnce,
    ExactlyOnce,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default)]
    pub authentication: KafkaConfigAuthentication,
    #[serde(default)]
    pub schema_registry_enum: Option<SchemaRegistryConfig>,
    #[serde(default)]
    pub connection_properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KafkaConfigAuthentication {
    #[serde(rename = "None")]
    None,
    #[serde(rename = "AWS_MSK_IAM")]
    AwsMskIam { region: String },
    #[serde(rename = "SASL")]
    Sasl {
        protocol: String,
        mechanism: String,
        username: String,
        password: String,
    },
}

impl Default for KafkaConfigAuthentication {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SchemaRegistryConfig {
    #[serde(rename = "None")]
    None,
    #[serde(rename = "Confluent Schema Registry")]
    ConfluentSchemaRegistry {
        endpoint: String,
        #[serde(rename = "apiKey")]
        api_key: Option<String>,
        #[serde(rename = "apiSecret")]
        api_secret: Option<String>,
    },
}
