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

// EventSerializer - Event serializer

use super::EndOfData;
use super::event::{Event, EventType};

/// EventSerializer - Event serializer
///
/// Provides unified event serialization and deserialization interface
pub struct EventSerializer;

impl EventSerializer {
    /// Serialize event to byte buffer
    pub fn to_serialized_event(
        event: &dyn Event,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send>> {
        event.serialize()
    }

    /// Deserialize event from byte buffer
    ///
    /// Buffer format: first byte is EventType, followed by Protocol Buffers encoded event data
    pub fn from_serialized_event(
        buffer: &[u8],
    ) -> Result<Box<dyn Event>, Box<dyn std::error::Error + Send>> {
        if buffer.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty buffer",
            )));
        }

        let event_type_byte = buffer[0];
        let event_type = EventType::from_u8(event_type_byte).ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown event type: {}", event_type_byte),
            )) as Box<dyn std::error::Error + Send>
        })?;

        // Starting from the second byte is Protocol Buffers encoded event data
        match event_type {
            EventType::EndOfData => {
                let (end_of_data, _) = EndOfData::deserialize_protobuf(buffer, 1)?;
                Ok(Box::new(end_of_data))
            }
        }
    }

    /// Get serialized size of event
    pub fn get_serialized_size(event: &dyn Event) -> usize {
        event.size()
    }
}
