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
