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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub service_id: String,
    pub service_name: String,
    pub version: String,
    pub host: String,
    pub port: u16,
    pub workers: Option<usize>,
    pub worker_multiplier: Option<usize>,
    pub debug: bool,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            service_id: "default-service".to_string(),
            service_name: "function-stream".to_string(),
            version: "0.1.0".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            workers: None,
            worker_multiplier: Some(4),
            debug: false,
        }
    }
}
