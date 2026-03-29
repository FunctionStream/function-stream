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

use bincode::{Decode, Encode};
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum Watermark {
    EventTime(SystemTime),
    Idle,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrowMessage {
    Data(RecordBatch),
    Signal(SignalMessage),
}

impl ArrowMessage {
    pub fn is_end(&self) -> bool {
        matches!(
            self,
            ArrowMessage::Signal(SignalMessage::Stop)
                | ArrowMessage::Signal(SignalMessage::EndOfData)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum SignalMessage {
    Barrier(CheckpointBarrier),
    Watermark(Watermark),
    Stop,
    EndOfData,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    pub epoch: u32,
    pub min_epoch: u32,
    pub timestamp: SystemTime,
    pub then_stop: bool,
}
