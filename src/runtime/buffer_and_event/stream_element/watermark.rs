// Watermark - Event time watermark
//
// Represents the progress of event time, telling operators that they should no longer receive elements with timestamps less than or equal to the watermark timestamp

use super::StreamElement;

/// Watermark - Event time watermark
/// 
/// Represents the progress of event time, telling operators that they should no longer receive elements with timestamps less than or equal to the watermark timestamp
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Watermark {
    /// Watermark timestamp (milliseconds)
    timestamp: u64,
}

impl Watermark {
    /// Create watermark
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Watermark representing end of event time
    pub fn max() -> Self {
        Self {
            timestamp: u64::MAX,
        }
    }

    /// Uninitialized watermark
    pub fn uninitialized() -> Self {
        Self {
            timestamp: u64::MIN,
        }
    }
}

impl StreamElement for Watermark {
    fn get_type(&self) -> super::StreamElementType {
        super::StreamElementType::Watermark
    }

    fn as_watermark(&self) -> Option<&Watermark> {
        Some(self)
    }
}

impl Watermark {
    /// Protocol Buffers serialization
    /// 
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message Watermark {
    ///     uint64 timestamp = 1;  // Watermark timestamp (milliseconds)
    /// }
    /// ```
    pub fn serialize_protobuf(&self, buffer: &mut [u8], offset: usize) -> Result<usize, Box<dyn std::error::Error + Send>> {
        crate::codec::encode_uint64_field(buffer, offset, 1, self.timestamp)
    }

    /// Size after Protocol Buffers serialization
    pub fn protobuf_size(&self) -> usize {
        crate::codec::compute_uint64_field_size(1, self.timestamp)
    }

    /// Protocol Buffers deserialization
    /// 
    /// Decode from the specified position in the byte array, returns (Watermark, bytes consumed)
    pub fn deserialize_protobuf(bytes: &[u8], offset: usize) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        let (field_number, timestamp, consumed) = crate::codec::decode_uint64_field(bytes, offset)?;
        if field_number != 1 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected field 1, got {}", field_number),
            )));
        }
        Ok((Watermark { timestamp }, consumed))
    }
}

