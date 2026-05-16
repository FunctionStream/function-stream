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

//! WebAssembly execution runtime.
//!
//! Implementation lives under `src/wasm/` in this package. It is currently **compiled as
//! part of the `function-stream` crate** via `#[path]` in `src/lib.rs` / `src/main.rs`, so paths
//! like `crate::sql` (streaming planner dependency) and `crate::memory` keep resolving.
//!
//! Operator state storage (`state_backend/`) also lives in this package and is compiled via
//! `#[path]` from the root crate as `crate::state_backend`.

pub const CRATE_NAME: &str = "function-stream-wasm-runtime";
