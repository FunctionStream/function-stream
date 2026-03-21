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

pub mod common;
pub mod api;

pub mod schema;
pub mod functions;
pub mod parse;
pub mod logical_node;
pub mod logical_planner;
pub mod analysis;
pub(crate) mod extensions;
pub mod types;

pub use schema::StreamSchemaProvider;
pub use parse::parse_sql;
pub use analysis::rewrite_plan;
pub use logical_planner::CompiledSql;

#[cfg(test)]
mod frontend_sql_coverage_tests;
