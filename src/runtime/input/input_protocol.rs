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

use crate::runtime::buffer_and_event::BufferOrEvent;
use std::time::Duration;

pub trait InputProtocol: Send + Sync + 'static {
    fn init(&self) -> Result<(), Box<dyn std::error::Error + Send>>;
    fn poll(
        &self,
        timeout: Duration,
    ) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>>;
    fn name(&self) -> String;
    fn on_start(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
    fn on_stop(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
    fn on_close(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
    fn on_checkpoint(&self, _id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
    fn on_checkpoint_finish(&self, _id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
