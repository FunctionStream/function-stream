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

#[derive(Debug, Clone)]
pub enum FunctionErrorStage {
    Input,
    Output,
    Processor,
}

#[derive(Debug, Clone)]
pub struct FunctionErrorReport {
    pub stage: FunctionErrorStage,
    pub index: usize,
    pub message: String,
}

impl FunctionErrorReport {
    pub fn input(index: usize, message: String) -> Self {
        Self {
            stage: FunctionErrorStage::Input,
            index,
            message,
        }
    }

    pub fn output(index: usize, message: String) -> Self {
        Self {
            stage: FunctionErrorStage::Output,
            index,
            message,
        }
    }

    pub fn processor(index: usize, message: String) -> Self {
        Self {
            stage: FunctionErrorStage::Processor,
            index,
            message,
        }
    }
}

impl std::fmt::Display for FunctionErrorReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stage = match &self.stage {
            FunctionErrorStage::Input => "input",
            FunctionErrorStage::Output => "output",
            FunctionErrorStage::Processor => "processor",
        };
        write!(f, "{}[{}]: {}", stage, self.index, self.message)
    }
}

/// Trait for reporting function errors into the task control path (e.g. via control MailBox).
pub trait ErrorReporter: Send + Sync {
    fn report_error(&self, report: FunctionErrorReport);
}
