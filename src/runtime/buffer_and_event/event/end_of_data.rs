// EndOfData - End of data event
//
// Indicates that there will be no more data records in the sub-partition, but other events may still be in transit

use super::event::{Event, EventType};

/// StopMode - Stop mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopMode {
    /// Normal stop, wait for all data to be consumed
    Drain,
    /// Immediate stop, do not wait for data to be consumed
    NoDrain,
}

/// EndOfData - End of data event
#[derive(Debug, Clone, Copy)]
pub struct EndOfData {
    mode: StopMode,
}

impl EndOfData {
    pub fn new(mode: StopMode) -> Self {
        Self { mode }
    }

    pub fn mode(&self) -> StopMode {
        self.mode
    }
}

impl Event for EndOfData {
    fn event_type(&self) -> EventType {
        EventType::EndOfData
    }

    /// Protocol Buffers serialization
    /// 
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message EndOfData {
    ///     uint32 mode = 1;  // 0=NoDrain, 1=Drain
    /// }
    /// ```
    fn serialize_protobuf(&self, buffer: &mut [u8], offset: usize) -> Result<usize, Box<dyn std::error::Error + Send>> {
        let mode_value = match self.mode {
            StopMode::Drain => 1u32,
            StopMode::NoDrain => 0u32,
        };
        crate::codec::encode_uint32_field(buffer, offset, 1, mode_value)
    }

    fn protobuf_size(&self) -> usize {
        crate::codec::compute_uint32_field_size(1, 1) // mode is at most 1
    }
}

impl EndOfData {
    /// Protocol Buffers deserialization
    /// 
    /// Decodes from the specified position in the byte array, returns (EndOfData, bytes consumed)
    /// 
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message EndOfData {
    ///     uint32 mode = 1;  // 0=NoDrain, 1=Drain
    /// }
    /// ```
    pub fn deserialize_protobuf(bytes: &[u8], offset: usize) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        let (field_number, mode_value, consumed) = crate::codec::decode_uint32_field(bytes, offset)?;
        if field_number != 1 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected field 1, got {}", field_number),
            )));
        }
        let mode = match mode_value {
            0 => StopMode::NoDrain,
            1 => StopMode::Drain,
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid StopMode value: {}", mode_value),
                )));
            }
        };
        Ok((EndOfData { mode }, consumed))
    }
}

