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

/// PulsarConfig - Pulsar consumer configuration
#[derive(Debug, Clone)]
pub struct PulsarConfig {
    pub url: String,
    pub topic: String,
    pub subscription: String,
    pub subscription_type: Option<String>,
    pub properties: HashMap<String, String>,
}

impl PulsarConfig {
    pub fn new(
        url: String,
        topic: String,
        subscription: String,
        subscription_type: Option<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            url,
            topic,
            subscription,
            subscription_type,
            properties,
        }
    }
}
