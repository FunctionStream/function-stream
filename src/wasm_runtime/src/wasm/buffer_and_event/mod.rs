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

// BufferAndEvent module - Buffer and event module
//
// Provides BufferOrEvent implementation, unified representation of data received from network or message queue
// Can be a buffer containing data records, or an event

mod buffer_or_event;
pub mod stream_element;

pub use buffer_or_event::BufferOrEvent;
// StreamRecord is now in the stream_element submodule, exported through stream_element
