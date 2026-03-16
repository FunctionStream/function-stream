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

use std::fmt;

/// Describes the role of a connection in the streaming pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectionType {
    Source,
    Sink,
    Lookup,
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "source"),
            ConnectionType::Sink => write!(f, "sink"),
            ConnectionType::Lookup => write!(f, "lookup"),
        }
    }
}

/// A connector operation that describes how to interact with an external system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorOp {
    pub connector: String,
    pub config: String,
    pub description: String,
}

impl ConnectorOp {
    pub fn new(connector: impl Into<String>, config: impl Into<String>) -> Self {
        let connector = connector.into();
        let description = connector.clone();
        Self {
            connector,
            config: config.into(),
            description,
        }
    }
}

/// Configuration for a connection profile (e.g., Kafka broker, Pulsar endpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionProfile {
    pub name: String,
    pub connector: String,
    pub config: std::collections::HashMap<String, String>,
}
