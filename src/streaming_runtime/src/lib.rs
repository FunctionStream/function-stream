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

//! Streaming execution runtime.
//!
//! The streaming engine and shared runtime helpers (`streaming/`, `util/`) are
//! implemented under [`src/streaming`] and [`src/util`] in this package. They are
//! currently **compiled as part of the `function-stream` crate** via `#[path]` in
//! `src/lib.rs` / `src/main.rs`, sharing the root `crate::sql` name (re-exported streaming planner crate).

pub const CRATE_NAME: &str = "function-stream-streaming-runtime";
