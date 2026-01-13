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

// StreamElement module - Stream element module
//
// Provides definitions for all stream elements in stream processing, including:
// - StreamElement trait (stream element base class)
// - StreamRecord (data record)
// - Watermark (event time watermark)
// - LatencyMarker (latency marker)
// - RecordAttributes (record attributes)
// - WatermarkStatus (watermark status)

mod latency_marker;
mod record_attributes;
mod stream_element;
mod stream_record;
mod watermark;
mod watermark_status;

pub use latency_marker::LatencyMarker;
pub use record_attributes::RecordAttributes;
pub use stream_element::{StreamElement, StreamElementType};
pub use stream_record::StreamRecord;
pub use watermark::Watermark;
pub use watermark_status::WatermarkStatus;
