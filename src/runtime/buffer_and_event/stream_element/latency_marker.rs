// LatencyMarker - Latency marker
//
// Marker used for measuring end-to-end latency

use super::StreamElement;

/// LatencyMarker - Latency marker
/// 
/// Marker used for measuring end-to-end latency
#[derive(Debug, Clone, Copy)]
pub struct LatencyMarker {
    /// Mark creation time (milliseconds)
    marked_time: u64,
    /// Operator ID
    operator_id: u32,
    /// Subtask index
    subtask_index: i32,
}

impl LatencyMarker {
    /// Create latency marker
    pub fn new(marked_time: u64, operator_id: u32, subtask_index: i32) -> Self {
        Self {
            marked_time,
            operator_id,
            subtask_index,
        }
    }

    /// Get marked time
    pub fn marked_time(&self) -> u64 {
        self.marked_time
    }

    /// Get operator ID
    pub fn operator_id(&self) -> u32 {
        self.operator_id
    }

    /// Get subtask index
    pub fn subtask_index(&self) -> i32 {
        self.subtask_index
    }
}

impl StreamElement for LatencyMarker {
    fn get_type(&self) -> super::StreamElementType {
        super::StreamElementType::LatencyMarker
    }

    fn as_latency_marker(&self) -> Option<&LatencyMarker> {
        Some(self)
    }
}

impl LatencyMarker {
    /// Protocol Buffers serialization
    /// 
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message LatencyMarker {
    ///     uint64 marked_time = 1;    // Mark creation time (milliseconds)
    ///     uint32 operator_id = 2;     // Operator ID
    ///     sint32 subtask_index = 3;   // Subtask index
    /// }
    /// ```
    pub fn serialize_protobuf(&self, buffer: &mut [u8], offset: usize) -> Result<usize, Box<dyn std::error::Error + Send>> {
        let mut pos = offset;
        
        // field 1: marked_time (uint64)
        pos += crate::codec::encode_uint64_field(buffer, pos, 1, self.marked_time)?;
        
        // field 2: operator_id (uint32)
        pos += crate::codec::encode_uint32_field(buffer, pos, 2, self.operator_id)?;
        
        // field 3: subtask_index (sint32)
        pos += crate::codec::encode_sint32_field(buffer, pos, 3, self.subtask_index)?;
        
        Ok(pos - offset)
    }

    /// Size after Protocol Buffers serialization
    pub fn protobuf_size(&self) -> usize {
        let mut size = 0;
        size += crate::codec::compute_uint64_field_size(1, self.marked_time);
        size += crate::codec::compute_uint32_field_size(2, self.operator_id);
        size += crate::codec::compute_sint32_field_size(3, self.subtask_index);
        size
    }

    /// Protocol Buffers deserialization
    /// 
    /// Decode from the specified position in the byte array, returns (LatencyMarker, bytes consumed)
    pub fn deserialize_protobuf(bytes: &[u8], offset: usize) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        use crate::codec::{decode_tag, decode_uint64_field, decode_uint32_field, decode_sint32_field, WireType};
        use crate::codec::decode_var_int64;
        use crate::codec::decode_byte_string;
        let mut pos = offset;
        let mut marked_time = None;
        let mut operator_id = None;
        let mut subtask_index = None;
        
        while pos < bytes.len() {
            let (field_number, wire_type, tag_consumed) = decode_tag(bytes, pos)?;
            pos += tag_consumed;
            
            match field_number {
                1 => {
                    let (_, value, consumed) = decode_uint64_field(bytes, pos - tag_consumed)?;
                    marked_time = Some(value);
                    pos = pos - tag_consumed + consumed;
                }
                2 => {
                    let (_, value, consumed) = decode_uint32_field(bytes, pos - tag_consumed)?;
                    operator_id = Some(value);
                    pos = pos - tag_consumed + consumed;
                }
                3 => {
                    let (_, value, consumed) = decode_sint32_field(bytes, pos - tag_consumed)?;
                    subtask_index = Some(value);
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
                        _ => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Unsupported wire type: {:?}", wire_type),
                            )));
                        }
                    }
                }
            }
        }
        
        Ok((LatencyMarker {
            marked_time: marked_time.ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing field: marked_time",
                )) as Box<dyn std::error::Error + Send>
            })?,
            operator_id: operator_id.ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing field: operator_id",
                )) as Box<dyn std::error::Error + Send>
            })?,
            subtask_index: subtask_index.ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing field: subtask_index",
                )) as Box<dyn std::error::Error + Send>
            })?,
        }, pos - offset))
    }
}

