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
mod drop_function;
mod show_functions;
mod start_function;
mod stop_function;
mod visitor;

pub use create_function::{CreateFunction, FunctionSource, ConfigSource};
pub use drop_function::DropFunction;
pub use show_functions::ShowFunctions;
pub use start_function::StartFunction;
pub use stop_function::StopFunction;
pub use visitor::{StatementVisitor, StatementVisitorContext, StatementVisitorResult};

use std::fmt;

#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub success: bool,
    pub message: String,
    pub data: Option<String>,
}

impl ExecuteResult {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: None,
        }
    }

    pub fn ok_with_data(message: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: Some(data.into()),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }
}

pub trait Statement: fmt::Debug + Send + Sync {
    fn accept(
        &self,
        visitor: &dyn StatementVisitor,
        context: &StatementVisitorContext,
    ) -> StatementVisitorResult;
}
