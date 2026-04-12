// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use thiserror::Error;
use crossbeam_channel::TrySendError;

#[derive(Error, Debug)]
pub enum StateEngineError {
    #[error("I/O error during state persistence: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Parquet serialization/deserialization failed: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Arrow computation failed: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Memory hard limit exceeded and spill channel is full")]
    MemoryBackpressureTimeout,

    #[error("Background I/O pool has been shut down or disconnected")]
    IoPoolDisconnected,

    #[error("State metadata corrupted: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, StateEngineError>;

impl<T> From<TrySendError<T>> for StateEngineError {
    fn from(err: TrySendError<T>) -> Self {
        match err {
            TrySendError::Full(_) => StateEngineError::MemoryBackpressureTimeout,
            TrySendError::Disconnected(_) => StateEngineError::IoPoolDisconnected,
        }
    }
}
