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

use std::collections::HashMap;

/// NatsConfig - NATS consumer configuration
#[derive(Debug, Clone)]
pub struct NatsConfig {
    /// NATS server URL(s), comma-separated
    pub url: String,
    /// Subject to subscribe to
    pub subject: String,
    /// Optional queue group for load balancing
    pub queue_group: Option<String>,
    /// Extra options (e.g. token, user, pass)
    pub properties: HashMap<String, String>,
}

impl NatsConfig {
    pub fn new(
        url: String,
        subject: String,
        queue_group: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            url,
            subject,
            queue_group,
            properties,
        }
    }
}
