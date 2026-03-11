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

//! RocksMQ input protocol (stub).
//!
//! RocksMQ (e.g. Milvus) has no official Rust client. This stub implements
//! InputProtocol but poll() always returns Ok(None). Replace with a real
//! consumer when a client is available (e.g. Milvus gRPC or future crate).

use super::config::RocksMQConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::input::input_protocol::InputProtocol;
use std::time::Duration;

pub struct RocksMQProtocol {
    config: RocksMQConfig,
}

impl RocksMQProtocol {
    pub fn new(config: RocksMQConfig) -> Self {
        Self { config }
    }
}

impl InputProtocol for RocksMQProtocol {
    fn name(&self) -> String {
        format!("rocksmq-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Stub: no-op until a real RocksMQ client is integrated.
        Ok(())
    }

    fn poll(
        &self,
        _timeout: Duration,
    ) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        // Stub: always no message. Replace with real RocksMQ consumer when available.
        Ok(None)
    }
}
