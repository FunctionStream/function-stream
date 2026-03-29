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

//! 接口层：算子与源实现需遵循的 trait 与运行时上下文。

pub mod context;
pub mod operator;
pub mod source;

pub use context::TaskContext;
pub use operator::{ConstructedOperator, MessageOperator, Registry};
pub use source::{SourceEvent, SourceOffset, SourceOperator};
