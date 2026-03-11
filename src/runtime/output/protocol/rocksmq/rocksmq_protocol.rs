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

//! RocksMQ output protocol (stub).
//!
//! RocksMQ has no official Rust client. This stub implements OutputProtocol
//! but send() is a no-op. Replace with a real producer when a client is available.

use super::producer_config::RocksMQProducerConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::output::output_protocol::OutputProtocol;

pub struct RocksMQOutputProtocol {
    config: RocksMQProducerConfig,
}

impl RocksMQOutputProtocol {
    pub fn new(config: RocksMQProducerConfig) -> Self {
        Self { config }
    }
}

impl OutputProtocol for RocksMQOutputProtocol {
    fn name(&self) -> String {
        format!("rocksmq-{}", self.config.topic)
    }

    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn send(&self, _data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Stub: no-op until a real RocksMQ producer is integrated.
        Ok(())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
