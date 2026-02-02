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

#[derive(Debug)]
pub struct BufferOrEvent {
    /// Buffer data (byte array, if is_buffer() returns true)
    buffer: Option<Vec<u8>>,
    /// Whether event has priority (can skip buffer)
    has_priority: bool,
    /// Channel/partition information (optional)
    channel_info: Option<String>,
    /// Size (bytes)
    size: usize,
    /// Whether more data is available
    more_available: bool,
    /// Whether more priority events are available
    more_priority_events: bool,
}

impl BufferOrEvent {
    /// Create BufferOrEvent of buffer type
    pub fn new_buffer(
        buffer: Vec<u8>,
        channel_info: Option<String>,
        more_available: bool,
        more_priority_events: bool,
    ) -> Self {
        let size = buffer.len();
        Self {
            buffer: Some(buffer),
            has_priority: false,
            channel_info,
            size,
            more_available,
            more_priority_events,
        }
    }

    /// Check if it's a buffer
    pub fn is_buffer(&self) -> bool {
        self.buffer.is_some()
    }

    /// Check if event has priority
    pub fn has_priority(&self) -> bool {
        self.has_priority
    }

    /// Get buffer data (if it's a buffer, returns reference to byte array)
    pub fn get_buffer(&self) -> Option<&[u8]> {
        self.buffer.as_deref()
    }

    /// Get buffer data ownership (if it's a buffer)
    pub fn into_buffer(self) -> Option<Vec<u8>> {
        self.buffer
    }

    /// Get channel/partition information
    pub fn get_channel_info(&self) -> Option<&str> {
        self.channel_info.as_deref()
    }

    /// Get size (bytes)
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Whether more data is available
    pub fn more_available(&self) -> bool {
        self.more_available
    }

    /// Whether more priority events are available
    pub fn more_priority_events(&self) -> bool {
        self.more_priority_events
    }

    /// Set whether more data is available
    pub fn set_more_available(&mut self, more_available: bool) {
        self.more_available = more_available;
    }
}
