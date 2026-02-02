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

// Watermark - Event time watermark
//
// Represents the progress of event time, telling operators that they should no longer receive elements with timestamps less than or equal to the watermark timestamp

use super::StreamElement;

/// Watermark - Event time watermark
///
/// Represents the progress of event time, telling operators that they should no longer receive elements with timestamps less than or equal to the watermark timestamp
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Watermark {
    /// Watermark timestamp (milliseconds)
    timestamp: u64,
}

impl Watermark {
    /// Create watermark
    #[allow(dead_code)]
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    /// Get timestamp
    #[allow(dead_code)]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Watermark representing end of event time
    #[allow(dead_code)]
    pub fn max() -> Self {
        Self {
            timestamp: u64::MAX,
        }
    }

    /// Uninitialized watermark
    #[allow(dead_code)]
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
    pub fn serialize_protobuf(
        &self,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize, Box<dyn std::error::Error + Send>> {
        crate::codec::encode_uint64_field(buffer, offset, 1, self.timestamp)
    }

    /// Size after Protocol Buffers serialization
    pub fn protobuf_size(&self) -> usize {
        crate::codec::compute_uint64_field_size(1, self.timestamp)
    }

    /// Protocol Buffers deserialization
    ///
    /// Decode from the specified position in the byte array, returns (Watermark, bytes consumed)
    pub fn deserialize_protobuf(
        bytes: &[u8],
        offset: usize,
    ) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
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
