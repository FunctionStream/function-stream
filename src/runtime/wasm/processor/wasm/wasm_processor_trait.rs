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

use crate::runtime::output::Output;
use crate::runtime::wasm::taskexecutor::InitContext;

pub trait WasmProcessor: Send + Sync {
    fn process(
        &self,
        data: Vec<u8>,
        input_index: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn process_watermark(
        &mut self,
        timestamp: u64,
        input_index: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!(
            "Processing watermark: {} from input {}",
            timestamp,
            input_index
        );
        Ok(())
    }

    fn init_with_context(
        &mut self,
        init_context: &InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    fn init_wasm_host(
        &mut self,
        _outputs: Vec<Box<dyn Output>>,
        _init_context: &InitContext,
        _task_name: String,
        _create_time: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!("Taking checkpoint: {}", checkpoint_id);
        Ok(())
    }

    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!("Finishing checkpoint: {}", checkpoint_id);
        Ok(())
    }

    fn restore_state(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!("Restoring state from checkpoint: {}", checkpoint_id);
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        true
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn start_outputs(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn stop_outputs(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn take_checkpoint_outputs(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!("Taking checkpoint for outputs: {}", checkpoint_id);
        Ok(())
    }

    fn finish_checkpoint_outputs(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::debug!("Finishing checkpoint for outputs: {}", checkpoint_id);
        Ok(())
    }

    fn close_outputs(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn set_error_state_outputs(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
