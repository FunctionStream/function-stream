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

// DataOutput - Data output interface
//
// Defines the standard interface for data output, used to send stream elements downstream

use crate::runtime::buffer_and_event::stream_element::{
    LatencyMarker, RecordAttributes, StreamRecord, Watermark, WatermarkStatus,
};

/// DataOutput - Data output interface
///
/// Defines methods for sending various stream elements downstream
pub trait DataOutput: Send + Sync {
    /// Emit record
    fn emit_record(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Emit watermark
    fn emit_watermark(
        &mut self,
        watermark: Watermark,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Emit watermark status
    fn emit_watermark_status(
        &mut self,
        status: WatermarkStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Emit latency marker
    fn emit_latency_marker(
        &mut self,
        marker: LatencyMarker,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    /// Emit record attributes
    fn emit_record_attributes(
        &mut self,
        attributes: RecordAttributes,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;
}

/// FinishedDataOutput - Finished data output
///
/// Used to represent output when input is finished, all methods are no-ops
pub struct FinishedDataOutput;

impl FinishedDataOutput {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FinishedDataOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl DataOutput for FinishedDataOutput {
    fn emit_record(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Input finished, ignore all output
        Ok(())
    }

    fn emit_watermark(
        &mut self,
        _watermark: Watermark,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn emit_watermark_status(
        &mut self,
        _status: WatermarkStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn emit_latency_marker(
        &mut self,
        _marker: LatencyMarker,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    fn emit_record_attributes(
        &mut self,
        _attributes: RecordAttributes,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
