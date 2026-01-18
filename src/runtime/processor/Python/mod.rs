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

// Python Processor module
//
// This module provides a Python-specific processor implementation
// that wraps the WASM processor for executing Python code compiled to WASM.

pub mod python_host;
pub mod python_service;

// Re-export commonly used types
pub use python_host::get_python_engine_and_component;
pub use python_service::PythonService;

