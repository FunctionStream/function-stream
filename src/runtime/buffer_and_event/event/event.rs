// Event - Event base class and interface
//
// Defines the base interface for the event system, reference Flink's AbstractEvent

use std::fmt::Debug;

/// EventType - Event type enumeration
///
/// Used to identify different types of events, used for serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    /// End of data event
    EndOfData = 1,
}

impl EventType {
    /// Create event type from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(EventType::EndOfData),
            _ => None,
        }
    }

    /// Convert to u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

pub trait Event: Send + Sync + Debug {
    /// Get event type
    fn event_type(&self) -> EventType;

    /// Serialize event to byte buffer (legacy interface, maintained for compatibility)
    fn serialize(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send>> {
        let size = self.protobuf_size();
        let mut buffer = vec![0u8; size];
        let written = self.serialize_protobuf(&mut buffer, 0)?;
        buffer.truncate(written);
        Ok(buffer)
    }

    /// Protocol Buffers serialization
    ///
    /// Writes to the specified position in the buffer, returns the number of bytes written
    ///
    /// # Protocol Buffers protocol
    /// All Events are serialized using Protocol Buffers format
    fn serialize_protobuf(
        &self,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize, Box<dyn std::error::Error + Send>>;

    /// Get event size (bytes)
    ///
    /// Used to estimate the size after serialization (Protocol Buffers format)
    fn size(&self) -> usize {
        self.protobuf_size()
    }

    /// Size after Protocol Buffers serialization
    fn protobuf_size(&self) -> usize;
}
