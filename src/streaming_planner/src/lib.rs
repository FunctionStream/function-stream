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

#![allow(dead_code)] // Planner keeps helpers/types for upcoming features; strict -D dead_code is noisy.

//! Streaming SQL planning (logical graph, connectors, schema, physical codec).
//!
//! The `function-stream` binary/library re-exports this crate as `function_stream::sql` for
//! stable `crate::sql::…` paths in in-tree modules.

pub mod api;
pub mod common;

pub mod analysis;
pub mod connector;
pub mod functions;
pub mod logical_node;
pub mod logical_planner;
pub mod parse;
pub mod physical;
pub mod planning_runtime;
pub mod schema;
pub mod types;

pub use analysis::rewrite_plan;
