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

//! RocksMQ input config.
//!
//! RocksMQ is an embedded message queue (e.g. used by Milvus). There is no
//! standalone Rust client at present; this is a stub. Replace with a real
//! implementation when a client (e.g. Milvus gRPC or native crate) is available.

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RocksMQConfig {
    pub path: String,
    pub topic: String,
    pub consumer_id: Option<String>,
    pub properties: HashMap<String, String>,
}

impl RocksMQConfig {
    pub fn new(
        path: String,
        topic: String,
        consumer_id: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            path,
            topic,
            consumer_id,
            properties,
        }
    }
}
