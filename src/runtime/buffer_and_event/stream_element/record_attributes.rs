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

// RecordAttributes - Record attributes
//
// Contains metadata attributes of records, such as whether it's backlog, etc.

use super::StreamElement;

/// RecordAttributes - Record attributes
///
/// Contains metadata attributes of records, such as whether it's backlog, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordAttributes {
    /// Whether processing backlog data
    is_backlog: bool,
}

impl RecordAttributes {
    /// Create record attributes
    pub fn new(is_backlog: bool) -> Self {
        Self { is_backlog }
    }

    /// Check if backlog
    pub fn is_backlog(&self) -> bool {
        self.is_backlog
    }

    /// Create default record attributes (non-backlog)
    pub fn default() -> Self {
        Self { is_backlog: false }
    }
}

impl StreamElement for RecordAttributes {
    fn get_type(&self) -> super::StreamElementType {
        super::StreamElementType::RecordAttributes
    }

    fn as_record_attributes(&self) -> Option<&RecordAttributes> {
        Some(self)
    }
}

impl RecordAttributes {
    /// Protocol Buffers serialization
    ///
    /// # Protocol Buffers protocol
    /// ```protobuf
    /// message RecordAttributes {
    ///     bool is_backlog = 1;  // Whether processing backlog data
    /// }
    /// ```
    pub fn serialize_protobuf(
        &self,
        buffer: &mut [u8],
        offset: usize,
    ) -> Result<usize, Box<dyn std::error::Error + Send>> {
        crate::codec::encode_bool_field(buffer, offset, 1, self.is_backlog)
    }

    /// Size after Protocol Buffers serialization
    pub fn protobuf_size(&self) -> usize {
        crate::codec::compute_bool_field_size(1)
    }

    /// Protocol Buffers deserialization
    ///
    /// Decode from the specified position in the byte array, returns (RecordAttributes, bytes consumed)
    pub fn deserialize_protobuf(
        bytes: &[u8],
        offset: usize,
    ) -> Result<(Self, usize), Box<dyn std::error::Error + Send>> {
        let (field_number, is_backlog, consumed) = crate::codec::decode_bool_field(bytes, offset)?;
        if field_number != 1 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected field 1, got {}", field_number),
            )));
        }
        Ok((RecordAttributes { is_backlog }, consumed))
    }
}

/// RecordAttributesBuilder - Record attributes builder
pub struct RecordAttributesBuilder {
    is_backlog: bool,
}

impl RecordAttributesBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self { is_backlog: false }
    }

    /// Set backlog status
    pub fn set_backlog(mut self, is_backlog: bool) -> Self {
        self.is_backlog = is_backlog;
        self
    }

    /// Build RecordAttributes
    pub fn build(self) -> RecordAttributes {
        RecordAttributes::new(self.is_backlog)
    }
}

impl Default for RecordAttributesBuilder {
    fn default() -> Self {
        Self::new()
    }
}
