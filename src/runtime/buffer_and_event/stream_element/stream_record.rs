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

// StreamRecord - Data record
//
// Represents a data record in the data stream, containing the actual data value and optional timestamp

use super::StreamElement;
use std::fmt::Debug;

/// StreamRecord - Data record
///
/// Represents a data record in the data stream, containing the actual data value (byte array) and optional timestamp
/// Note: StreamRecord is not an Event, but serialization support is required
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct StreamRecord {
    /// Actual data value (byte array)
    value: Vec<u8>,
    /// Record timestamp (milliseconds)
    timestamp: Option<u64>,
}

impl StreamRecord {
    /// Create record without timestamp
    #[allow(dead_code)]
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            value,
            timestamp: None,
        }
    }

    /// Create record with timestamp
    #[allow(dead_code)]
    pub fn with_timestamp(value: Vec<u8>, timestamp: u64) -> Self {
        Self {
            value,
            timestamp: Some(timestamp),
        }
    }

    /// Get data value (reference to byte array)
    #[allow(dead_code)]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Get data value (ownership of byte array)
    #[allow(dead_code)]
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    /// Get timestamp
    ///
    /// Returns None if there is no timestamp
    #[allow(dead_code)]
    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    /// Get timestamp, returns u64::MIN if not present
    #[allow(dead_code)]
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp.unwrap_or(u64::MIN)
    }

    /// Check if has timestamp
    #[allow(dead_code)]
    pub fn has_timestamp(&self) -> bool {
        self.timestamp.is_some()
    }

    /// Replace value (preserve timestamp)
    #[allow(dead_code)]
    pub fn replace(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }

    /// Replace value and timestamp
    #[allow(dead_code)]
    pub fn replace_with_timestamp(mut self, value: Vec<u8>, timestamp: u64) -> Self {
        self.value = value;
        self.timestamp = Some(timestamp);
        self
    }
}

/// StreamRecord implements StreamElement trait
impl StreamElement for StreamRecord {
    fn get_type(&self) -> super::StreamElementType {
        super::StreamElementType::Record
    }

    fn as_record(&self) -> Option<&StreamRecord> {
        Some(self)
    }
}

/// StreamRecordSerializable - StreamRecord serialization interface
///
/// Serialization interface implemented by StreamRecord itself, used for getting type and serialization
#[allow(dead_code)]
pub trait StreamRecordSerializable: Send + Sync + Debug {
    /// Get StreamRecord type
    fn record_type(&self) -> StreamRecordType;

    /// Serialize StreamRecord
    /// Write to the specified position in the buffer, returns bytes written
    fn serialize(
        &self,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize, Box<dyn std::error::Error + Send>>;

    /// Get serialized size
    fn serialized_size(&self) -> usize;
}

impl StreamRecordSerializable for StreamRecord {
    fn record_type(&self) -> StreamRecordType {
        if self.has_timestamp() {
            StreamRecordType::RecordWithTimestamp
        } else {
            StreamRecordType::RecordWithoutTimestamp
        }
    }

    /// Protocol Buffers serialization
    ///
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message StreamRecord {
    ///     bytes value = 1;        // Data value (byte array)
    ///     uint64 timestamp = 2;  // Timestamp (optional, written if present)
    /// }
    /// ```
    fn serialize(
        &self,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize, Box<dyn std::error::Error + Send>> {
        let mut pos = offset;

        // field 1: value (bytes)
        pos += crate::codec::encode_bytes_field(buffer, pos, 1, &self.value)?;

        // field 2: timestamp (uint64, optional)
        if let Some(timestamp) = self.timestamp {
            pos += crate::codec::encode_uint64_field(buffer, pos, 2, timestamp)?;
        }

        Ok(pos - offset)
    }

    fn serialized_size(&self) -> usize {
        let mut size = 0;
        // field 1: value (bytes)
        size += crate::codec::compute_bytes_field_size(1, self.value.len());
        // field 2: timestamp (uint64, optional)
        if self.timestamp.is_some() {
            size += crate::codec::compute_uint64_field_size(2, self.timestamp.unwrap());
        }
        size
    }
}

impl StreamRecord {
    /// Protocol Buffers deserialization
    ///
    /// Decode from the specified position in the byte array, returns (StreamRecord, bytes consumed)
    ///
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message StreamRecord {
    ///     bytes value = 1;        // Data value (byte array)
    ///     uint64 timestamp = 2;  // Timestamp (optional, written if present)
    /// }
    /// ```
    #[allow(dead_code)]
    pub fn deserialize_protobuf(
        bytes: &[u8],
        offset: usize,
    ) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        use crate::codec::decode_byte_string;
        use crate::codec::decode_var_int64;
        use crate::codec::{WireType, decode_bytes_field, decode_tag, decode_uint64_field};

        let mut pos = offset;
        let mut value = None;
        let mut timestamp = None;

        while pos < bytes.len() {
            let (field_number, wire_type, tag_consumed) = decode_tag(bytes, pos)?;
            pos += tag_consumed;

            match field_number {
                1 => {
                    // field 1: value (bytes)
                    let (_, bytes_value, consumed) = decode_bytes_field(bytes, pos - tag_consumed)?;
                    value = Some(bytes_value);
                    pos = pos - tag_consumed + consumed;
                }
                2 => {
                    // field 2: timestamp (uint64, optional)
                    let (_, timestamp_value, consumed) =
                        decode_uint64_field(bytes, pos - tag_consumed)?;
                    timestamp = Some(timestamp_value);
                    pos = pos - tag_consumed + consumed;
                }
                _ => {
                    // Skip unknown fields
                    match wire_type {
                        WireType::Varint => {
                            let (_, consumed) = decode_var_int64(bytes, pos)?;
                            pos += consumed;
                        }
                        WireType::LengthDelimited => {
                            let (_, consumed) = decode_byte_string(bytes, pos)?;
                            pos += consumed;
                        }
                        WireType::Fixed64 => {
                            pos += 8;
                        }
                        WireType::Fixed32 => {
                            pos += 4;
                        }
                        WireType::StartGroup | WireType::EndGroup => {
                            // Deprecated group type, not supported
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Unsupported,
                                format!("Unsupported wire type: {:?}", wire_type),
                            )));
                        }
                    }
                }
            }
        }

        let value = value.ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing field: value",
            )) as Box<dyn std::error::Error + Send>
        })?;

        Ok((StreamRecord { value, timestamp }, pos - offset))
    }
}

/// Deserialize StreamRecord (backward compatibility helper function)
///
/// Decode from the specified position in the byte array, returns (StreamRecord, bytes consumed)
#[allow(dead_code)]
#[deprecated(note = "Use StreamRecord::deserialize_protobuf instead")]
pub fn deserialize_stream_record(
    bytes: &[u8],
    offset: usize,
) -> Result<(StreamRecord, usize), Box<dyn std::error::Error + Send>> {
    StreamRecord::deserialize_protobuf(bytes, offset)
}

/// StreamRecordType - StreamRecord type marker
///
/// Used to identify the type of StreamRecord during serialization
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamRecordType {
    /// Record with timestamp
    RecordWithTimestamp = 1,
    /// Record without timestamp
    RecordWithoutTimestamp = 2,
}

impl StreamRecordType {
    /// Create type from u8
    #[allow(dead_code)]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(StreamRecordType::RecordWithTimestamp),
            2 => Some(StreamRecordType::RecordWithoutTimestamp),
            _ => None,
        }
    }

    /// Convert to u8
    #[allow(dead_code)]
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}
