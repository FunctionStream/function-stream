// StreamElement - Stream element base class
//
// Defines the base interface for all stream elements

use std::fmt::Debug;

/// StreamElementType - Stream element type enumeration
///
/// Used to identify different types of stream elements
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamElementType {
    /// Data record
    Record,
    /// Event time watermark
    Watermark,
    /// Latency marker
    LatencyMarker,
    /// Record attributes
    RecordAttributes,
    /// Watermark status
    WatermarkStatus,
}

/// StreamElement - Base class for all stream elements
///
/// Defines type checking and conversion methods for stream elements
pub trait StreamElement: Send + Sync + Debug {
    /// Get stream element type
    fn get_type(&self) -> StreamElementType;

    /// Convert to record (if possible)
    fn as_record(&self) -> Option<&super::StreamRecord> {
        None
    }

    /// Convert to watermark (if possible)
    fn as_watermark(&self) -> Option<&super::Watermark> {
        None
    }

    /// Convert to latency marker (if possible)
    fn as_latency_marker(&self) -> Option<&super::LatencyMarker> {
        None
    }

    /// Convert to record attributes (if possible)
    fn as_record_attributes(&self) -> Option<&super::RecordAttributes> {
        None
    }

    /// Convert to watermark status (if possible)
    fn as_watermark_status(&self) -> Option<&super::WatermarkStatus> {
        None
    }
}
