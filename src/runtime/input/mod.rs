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

// Input module - Input module
//
// Provides input implementations for various data sources, including:
// - Input source interface
// - Input source provider (creates input sources from configuration)
// - Input source protocols (Kafka, etc.)

mod input_source;
mod input_source_provider;
pub mod protocol;

pub use input_source::{InputSource, InputSourceState};
pub use input_source_provider::InputSourceProvider;
