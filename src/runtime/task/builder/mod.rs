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

// Builder module - Task builder module
//
// Provides different types of task builders:
// - TaskBuilder: Main builder that dispatches to corresponding builders based on configuration type
// - ProcessorBuilder: Processor type task builder
// - SourceBuilder: Source type task builder (future support)
// - SinkBuilder: Sink type task builder (future support)

mod processor;
mod python;
mod sink;
mod source;
mod task_builder;

pub use task_builder::TaskBuilder;
