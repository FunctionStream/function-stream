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
use crate::runtime::taskexecutor::InitContext;

pub trait Output: Send + Sync {
    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn collect(&mut self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn restore_state(
        &mut self,
        _checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Output>;

    fn set_error_state(&self) -> Result<(), Box<dyn std::error::Error + Send>>;
}
