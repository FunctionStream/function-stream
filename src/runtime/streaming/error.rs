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

use std::fmt::Display;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Operator execution failed: {0:#}")]
    Operator(#[from] anyhow::Error),

    #[error("Downstream send failed: {0}")]
    DownstreamSend(String),

    #[error("Internal engine error: {0}")]
    Internal(String),

    #[error("State backend error: {0}")]
    State(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl RunError {
    pub fn internal<T: Display>(msg: T) -> Self {
        Self::Internal(msg.to_string())
    }

    pub fn downstream<T: Display>(msg: T) -> Self {
        Self::DownstreamSend(msg.to_string())
    }

    pub fn state<T: Display>(msg: T) -> Self {
        Self::State(msg.to_string())
    }
}
