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

// Output module - Output module
//
// Provides output implementations, including:
// - Output sink interface
// - Output sink provider (creates output sinks from configuration)
// - Output protocols (Kafka, etc.)

mod output_sink;
mod output_sink_provider;
mod protocol;

pub use output_sink::OutputSink;
pub use output_sink_provider::OutputSinkProvider;
