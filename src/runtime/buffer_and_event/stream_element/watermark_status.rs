// WatermarkStatus - Watermark status
//
// Represents the status of watermark (IDLE/ACTIVE)

use super::StreamElement;

/// WatermarkStatus - Watermark status
/// 
/// Represents the status of watermark (IDLE/ACTIVE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkStatus {
    /// Idle status: no data arriving
    Idle,
    /// Active status: data arriving
    Active,
}

impl WatermarkStatus {
    /// Check if idle status
    pub fn is_idle(&self) -> bool {
        matches!(self, WatermarkStatus::Idle)
    }

    /// Check if active status
    pub fn is_active(&self) -> bool {
        matches!(self, WatermarkStatus::Active)
    }
}

impl StreamElement for WatermarkStatus {
    fn get_type(&self) -> super::StreamElementType {
        super::StreamElementType::WatermarkStatus
    }

    fn as_watermark_status(&self) -> Option<&WatermarkStatus> {
        Some(self)
    }
}

impl WatermarkStatus {
    /// Protocol Buffers serialization
    /// 
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message WatermarkStatus {
    ///     uint32 status = 1;  // 0=Idle, 1=Active
    /// }
    /// ```
    pub fn serialize_protobuf(&self, buffer: &mut [u8], offset: usize) -> Result<usize, Box<dyn std::error::Error + Send>> {
        let status_value = match self {
            WatermarkStatus::Idle => 0u32,
            WatermarkStatus::Active => 1u32,
        };
        crate::codec::encode_uint32_field(buffer, offset, 1, status_value)
    }

    /// Size after Protocol Buffers serialization
    pub fn protobuf_size(&self) -> usize {
        crate::codec::compute_uint32_field_size(1, 1) // status is at most 1
    }

    /// Protocol Buffers deserialization
    /// 
    /// Decode from the specified position in the byte array, returns (WatermarkStatus, bytes consumed)
    pub fn deserialize_protobuf(bytes: &[u8], offset: usize) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        let (field_number, status_value, consumed) = crate::codec::decode_uint32_field(bytes, offset)?;
        if field_number != 1 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected field 1, got {}", field_number),
            )));
        }
        let status = match status_value {
            0 => WatermarkStatus::Idle,
            1 => WatermarkStatus::Active,
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid WatermarkStatus value: {}", status_value),
                )));
            }
        };
        Ok((status, consumed))
    }
}

