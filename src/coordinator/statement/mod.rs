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

mod create_function;
mod create_python_function;
mod create_table;
mod drop_function;
mod insert_statement;
mod show_functions;
mod start_function;
mod stop_function;
mod visitor;

pub use create_function::{ConfigSource, CreateFunction, FunctionSource};
pub use create_python_function::{CreatePythonFunction, PythonModule};
pub use create_table::CreateTable;
pub use drop_function::DropFunction;
pub use insert_statement::InsertStatement;
pub use show_functions::ShowFunctions;
pub use start_function::StartFunction;
pub use stop_function::StopFunction;
pub use visitor::{StatementVisitor, StatementVisitorContext, StatementVisitorResult};

use std::fmt;

pub trait Statement: fmt::Debug + Send + Sync {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;
}
