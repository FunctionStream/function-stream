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

// IO module - Stream task input/output module
// - Read data from network or sources
// - Data deserialization
// - Checkpoint barrier handling
// - Multi-input stream selection (multi-input only)
// - Data output to network

mod availability;
mod data_input_status;
mod data_output;
mod input_processor;
mod input_selection;
mod key_selection;
mod multiple_input_processor;
mod task_input;

// Re-export stream_element package contents
pub use availability::*;
pub use data_input_status::*;
pub use data_output::*;
pub use input_processor::*;

// Re-export input module contents for backward compatibility
