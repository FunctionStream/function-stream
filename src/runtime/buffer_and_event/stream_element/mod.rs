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
