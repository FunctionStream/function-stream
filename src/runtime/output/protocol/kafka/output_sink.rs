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

//! KafkaOutputSink - Kafka output sink implementation
//!
//! Implements OutputSink for sending data to Kafka message queue.
//! Uses rdkafka client library for actual Kafka production.

use super::producer_config::KafkaProducerConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::output::OutputSink;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use std::sync::Arc;
use std::sync::Mutex;

// ==================== Constants ====================

/// Default channel capacity (maximum messages in bounded channel)
const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

/// Maximum batch size per consume (prevents control signals from being blocked)
const MAX_BATCH_CONSUME_SIZE: usize = 100;

/// Default flush timeout (milliseconds)
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 5000;

/// Control operation timeout (milliseconds)
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;

/// Maximum retries for control operations
const CONTROL_OPERATION_MAX_RETRIES: u32 = 5;

// ==================== Enum Definitions ====================

/// Sink control signal (control layer)
///
/// Each signal includes a `completion_flag` to track task completion.
#[derive(Debug, Clone)]
enum SinkControlSignal {
    /// Start signal
    Start { completion_flag: TaskCompletionFlag },
    /// Stop signal
    Stop { completion_flag: TaskCompletionFlag },
    /// Close signal
    Close { completion_flag: TaskCompletionFlag },
    /// Begin checkpoint signal
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// End checkpoint signal
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// Flush signal
    Flush { completion_flag: TaskCompletionFlag },
}

/// Control signal processing result
enum ControlAction {
    /// Continue running (process data)
    Continue,
    /// Pause (stop processing data, block waiting for control signals)
    Pause,
    /// Exit thread
    Exit,
}

// ==================== Struct Definitions ====================

/// KafkaOutputSink - Kafka output sink
///
/// Uses a dedicated thread for data sending, with internal data cache Channel.
/// Architecture:
/// - Main thread puts data into Channel
/// - Send thread consumes data from Channel and sends to Kafka
/// - Supports control signals (stop, close, checkpoint)
/// - State changes are managed uniformly by runloop thread (except init)
pub struct KafkaOutputSink {
    /// Kafka configuration
    config: KafkaProducerConfig,
    /// Output sink ID (starting from 0, identifies different output sinks)
    sink_id: usize,
    /// Component state (shared, managed uniformly by runloop thread)
    state: Arc<Mutex<ComponentState>>,
    /// Data send thread
    send_thread: Option<std::thread::JoinHandle<()>>,
    /// Data cache Channel sender (main thread writes)
    data_sender: Option<crossbeam_channel::Sender<BufferOrEvent>>,
    /// Data cache Channel receiver (send thread reads)
    data_receiver: Option<crossbeam_channel::Receiver<BufferOrEvent>>,
    /// Control signal Channel sender
    control_sender: Option<crossbeam_channel::Sender<SinkControlSignal>>,
    /// Control signal Channel receiver
    control_receiver: Option<crossbeam_channel::Receiver<SinkControlSignal>>,
}

impl KafkaOutputSink {
    // ==================== Configuration/Construction ====================

    /// Create a new Kafka output sink
    ///
    /// # Arguments
    /// - `config`: Kafka configuration
    /// - `sink_id`: Output sink ID (starting from 0, identifies different output sinks)
    pub fn new(config: KafkaProducerConfig, sink_id: usize) -> Self {
        Self {
            config,
            sink_id,
            state: Arc::new(Mutex::new(ComponentState::Uninitialized)),
            send_thread: None,
            data_sender: None,
            data_receiver: None,
            control_sender: None,
            control_receiver: None,
        }
    }

    /// Create from configuration
    ///
    /// # Arguments
    /// - `config`: Kafka configuration
    /// - `sink_id`: Output sink ID (starting from 0, identifies different output sinks)
    pub fn from_config(config: KafkaProducerConfig, sink_id: usize) -> Self {
        Self::new(config, sink_id)
    }

    // ==================== Timeout Retry Helper Functions ====================

    /// Wait for control operation completion with timeout and retry
    ///
    /// Waits for completion_flag to be marked complete and checks operation result.
    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = std::time::Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    // Check operation result
                    if let Some(error) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(format!(
                            "{} failed: {}",
                            operation_name, error
                        ))));
                    }
                    return Ok(());
                }
                Err(_) => {
                    log::warn!(
                        "{} timeout (retry {}/{}), topic: {}",
                        operation_name,
                        retry + 1,
                        CONTROL_OPERATION_MAX_RETRIES,
                        self.config.topic
                    );
                }
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "{} failed after {} retries",
                operation_name, CONTROL_OPERATION_MAX_RETRIES
            ),
        )))
    }

    #[allow(dead_code)]
    fn wait_flush_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = std::time::Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    // Check operation result
                    if let Some(error) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(format!(
                            "Flush failed: {}",
                            error
                        ))));
                    }
                    return Ok(());
                }
                Err(_) => {
                    log::warn!(
                        "Flush timeout (retry {}/{}), topic: {}",
                        retry + 1,
                        CONTROL_OPERATION_MAX_RETRIES,
                        self.config.topic
                    );
                }
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "Flush failed after {} retries",
                CONTROL_OPERATION_MAX_RETRIES
            ),
        )))
    }

    // ==================== Send Thread Main Loop ====================

    /// Send thread main loop
    ///
    /// State machine driven event loop:
    /// - Running state: waits for both control signals and data
    /// - Paused state: blocks waiting only for control signals
    /// - All state changes are handled uniformly in this thread
    fn send_thread_loop(
        producer: ThreadedProducer<DefaultProducerContext>,
        data_receiver: crossbeam_channel::Receiver<BufferOrEvent>,
        control_receiver: crossbeam_channel::Receiver<SinkControlSignal>,
        state: Arc<Mutex<ComponentState>>,
        config: KafkaProducerConfig,
    ) {
        use crossbeam_channel::select;

        // Initial state is paused, waiting for Start signal
        let mut is_running = false;
        log::debug!(
            "Send thread started (paused), waiting for start signal for topic: {}",
            config.topic
        );

        loop {
            if is_running {
                // ========== Running state: wait for both control signals and data ==========
                select! {
                    recv(control_receiver) -> result => {
                        match result {
                            Ok(signal) => {
                                match Self::handle_control_signal(signal, &producer, &data_receiver, &state, &config) {
                                    ControlAction::Continue => is_running = true,
                                    ControlAction::Pause => {
                                        is_running = false;
                                        log::info!("Sink paused for topic: {}", config.topic);
                                    }
                                    ControlAction::Exit => break,
                                }
                            }
                            Err(_) => {
                                log::warn!("Control channel disconnected for topic: {}", config.topic);
                                break;
                            }
                        }
                    }
                    recv(data_receiver) -> result => {
                        match result {
                            Ok(data) => {
                                // Send current message
                                Self::send_message(&producer, data, &config);

                                // Non-blocking consume and send more data, reduces select! scheduling overhead
                                // Limit batch size to ensure control signals are processed in time
                                let mut batch_count = 1;
                                while batch_count < MAX_BATCH_CONSUME_SIZE {
                                    match data_receiver.try_recv() {
                                        Ok(more_data) => {
                                            Self::send_message(&producer, more_data, &config);
                                            batch_count += 1;
                                        }
                                        Err(_) => break,
                                    }
                                }

                                // Flush after batch ends, ensures messages are sent to Kafka
                                Self::flush_producer(&producer);
                            }
                            Err(_) => {
                                log::info!("Data channel disconnected for topic: {}", config.topic);
                                break;
                            }
                        }
                    }
                }
            } else {
                // ========== Paused state: block waiting only for control signals ==========
                match control_receiver.recv() {
                    Ok(signal) => {
                        match Self::handle_control_signal(
                            signal,
                            &producer,
                            &data_receiver,
                            &state,
                            &config,
                        ) {
                            ControlAction::Continue => is_running = true,
                            ControlAction::Pause => is_running = false,
                            ControlAction::Exit => break,
                        }
                    }
                    Err(_) => {
                        log::warn!("Control channel disconnected for topic: {}", config.topic);
                        break;
                    }
                }
            }
        }

        log::info!("Send thread exiting for topic: {}", config.topic);
    }

    // ==================== Control Layer Functions ====================

    /// Handle control signal (executed in runloop thread, manages state changes and checks uniformly)
    fn handle_control_signal(
        signal: SinkControlSignal,
        producer: &ThreadedProducer<DefaultProducerContext>,
        data_receiver: &crossbeam_channel::Receiver<BufferOrEvent>,
        state: &Arc<Mutex<ComponentState>>,
        config: &KafkaProducerConfig,
    ) -> ControlAction {
        let current_state = state.lock().unwrap().clone();

        match signal {
            SinkControlSignal::Start { completion_flag } => {
                // Only Initialized or Stopped state can Start
                if !matches!(
                    current_state,
                    ComponentState::Initialized | ComponentState::Stopped
                ) {
                    let error = format!("Cannot start in state: {:?}", current_state);
                    log::error!("{} for topic: {}", error, config.topic);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::debug!("Sink start signal received for topic: {}", config.topic);
                *state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SinkControlSignal::Stop { completion_flag } => {
                // Only Running or Checkpointing state can Stop
                if !matches!(
                    current_state,
                    ComponentState::Running | ComponentState::Checkpointing
                ) {
                    // Stop operation silently succeeds if state is wrong (idempotent)
                    log::debug!(
                        "Stop ignored in state: {:?} for topic: {}",
                        current_state,
                        config.topic
                    );
                    completion_flag.mark_completed();
                    return ControlAction::Pause;
                }
                log::info!("Sink stop signal received for topic: {}", config.topic);
                *state.lock().unwrap() = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }
            SinkControlSignal::Close { completion_flag } => {
                // Close can be executed in any state
                log::info!("Sink close signal received for topic: {}", config.topic);
                *state.lock().unwrap() = ComponentState::Closing;
                Self::drain_remaining_data(producer, data_receiver, config);
                Self::flush_producer(producer);
                *state.lock().unwrap() = ComponentState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            SinkControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                // Only Running state can Checkpoint
                if !matches!(current_state, ComponentState::Running) {
                    let error = format!("Cannot take checkpoint in state: {:?}", current_state);
                    log::error!("{} for topic: {}", error, config.topic);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!(
                    "Checkpoint {} started for topic: {}",
                    checkpoint_id,
                    config.topic
                );
                *state.lock().unwrap() = ComponentState::Checkpointing;
                Self::drain_remaining_data(producer, data_receiver, config);
                Self::flush_producer(producer);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SinkControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                // Only Checkpointing state can CheckpointFinish
                if !matches!(current_state, ComponentState::Checkpointing) {
                    let error = format!("Cannot finish checkpoint in state: {:?}", current_state);
                    log::error!("{} for topic: {}", error, config.topic);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!(
                    "Checkpoint {} finish for topic: {}",
                    checkpoint_id,
                    config.topic
                );
                *state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SinkControlSignal::Flush { completion_flag } => {
                log::info!("Sink flush signal received for topic: {}", config.topic);
                Self::drain_remaining_data(producer, data_receiver, config);
                Self::flush_producer(producer);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
        }
    }

    /// Drain all remaining data from data channel when closing, prevents leakage
    fn drain_remaining_data(
        producer: &ThreadedProducer<DefaultProducerContext>,
        data_receiver: &crossbeam_channel::Receiver<BufferOrEvent>,
        config: &KafkaProducerConfig,
    ) {
        let mut drained_count = 0;

        // Non-blocking drain and send all remaining data
        while let Ok(data) = data_receiver.try_recv() {
            Self::send_message(producer, data, config);
            drained_count += 1;
        }

        if drained_count > 0 {
            log::info!(
                "Drained {} remaining messages before closing for topic: {}",
                drained_count,
                config.topic
            );
        }
    }

    /// Flush Kafka Producer
    fn flush_producer(producer: &ThreadedProducer<DefaultProducerContext>) {
        let _ = producer.flush(std::time::Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS));
    }

    // ==================== Data Layer Functions ====================

    /// Send single message
    ///
    /// ThreadedProducer handles batching internally, no manual batching needed.
    #[inline]
    fn send_message(
        producer: &ThreadedProducer<DefaultProducerContext>,
        data: BufferOrEvent,
        config: &KafkaProducerConfig,
    ) {
        // Use into_buffer() to take ownership, avoids extra copy
        if let Some(payload) = data.into_buffer() {
            let payload_str = String::from_utf8_lossy(&payload);
            log::info!(
                "Sending to Kafka topic '{}': len={}, payload={}",
                config.topic,
                payload.len(),
                payload_str
            );

            let mut record: BaseRecord<'_, (), Vec<u8>> =
                BaseRecord::to(&config.topic).payload(&payload);

            if let Some(partition) = config.partition {
                record = record.partition(partition);
            }

            if let Err((e, _)) = producer.send(record) {
                log::error!(
                    "Failed to send message to Kafka topic {}: {}",
                    config.topic,
                    e
                );
            }
        }
    }
}

// ==================== OutputSink Trait Implementation ====================

impl OutputSink for KafkaOutputSink {
    // -------------------- init --------------------

    fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // init_with_context is the only method that sets state in caller thread (runloop thread not started yet)
        if !matches!(*self.state.lock().unwrap(), ComponentState::Uninitialized) {
            return Ok(());
        }

        // Validate configuration
        if self.config.bootstrap_servers.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kafka bootstrap_servers is empty",
            )));
        }
        if self.config.topic.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kafka topic is empty",
            )));
        }

        // Create Channels
        let (data_sender, data_receiver) = crossbeam_channel::bounded(DEFAULT_CHANNEL_CAPACITY);
        let (control_sender, control_receiver) = crossbeam_channel::bounded(10);
        self.data_sender = Some(data_sender);
        self.control_sender = Some(control_sender);

        // Create Kafka producer and start thread
        let producer = self.config.create_producer()?;
        let config_clone = self.config.clone();
        let state_clone = self.state.clone();

        let thread_name = format!("kafka-sink-{}-{}", self.sink_id, self.config.topic);
        let thread_handle = std::thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                Self::send_thread_loop(
                    producer,
                    data_receiver,
                    control_receiver,
                    state_clone,
                    config_clone,
                );
            })
            .map_err(|e| -> Box<dyn std::error::Error + Send> {
                Box::new(std::io::Error::other(format!(
                    "Failed to start thread: {}",
                    e
                )))
            })?;

        // Register thread group to InitContext
        use crate::runtime::processor::wasm::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut output_thread_group = ThreadGroup::new(
            ThreadGroupType::OutputSink(self.sink_id),
            format!("OutputSink-{}", self.sink_id),
        );
        output_thread_group.add_thread(thread_handle);
        init_context.register_thread_group(output_thread_group);

        // Note: thread handle has been moved to thread group, no longer stored in send_thread
        // When closing, thread management is done through TaskHandle
        self.send_thread = None;
        // init_with_context is the only place that sets state in caller thread
        *self.state.lock().unwrap() = ComponentState::Initialized;
        Ok(())
    }

    // -------------------- start --------------------

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, let runloop thread's handle_control_signal handle it
        // Send signal to runloop thread, let runloop thread set state
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Start {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send start signal: {}",
                        e
                    )))
                })?;
        }

        // Wait with timeout retry for runloop thread to complete
        self.wait_with_retry(&completion_flag, "Start")?;

        log::debug!(
            "KafkaOutputSink started: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- stop --------------------

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, let runloop thread's handle_control_signal handle it
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Stop {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send stop signal: {}",
                        e
                    )))
                })?;
        }

        // Wait with timeout retry for runloop thread to complete
        self.wait_with_retry(&completion_flag, "Stop")?;

        log::info!(
            "KafkaOutputSink stopped: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- checkpoint --------------------

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, let runloop thread's handle_control_signal handle it
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender
                .send(signal)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Checkpoint signal failed: {}",
                        e
                    )))
                })?;
        }

        self.wait_with_retry(&completion_flag, "Checkpoint")?;

        log::info!(
            "Checkpoint {} started: sink_id={}, topic={}",
            checkpoint_id,
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, let runloop thread's handle_control_signal handle it
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender
                .send(signal)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send checkpoint finish signal: {}",
                        e
                    )))
                })?;
        }

        // Wait with timeout retry for runloop thread to complete
        self.wait_with_retry(&completion_flag, "CheckpointFinish")?;

        log::info!(
            "Checkpoint {} finished: sink_id={}, topic={}",
            checkpoint_id,
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- close --------------------

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // State check (read only)
        if matches!(*self.state.lock().unwrap(), ComponentState::Closed) {
            return Ok(());
        }

        // Send signal to runloop thread, let runloop thread set state
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::Close {
                completion_flag: completion_flag.clone(),
            };
            if control_sender.send(signal).is_ok() {
                // Wait with timeout retry for runloop thread to complete
                // Close operation allows failure (thread may have exited), so ignore errors
                let _ = self.wait_with_retry(&completion_flag, "Close");
            }
        }

        // Note: thread handle has been moved to thread group, managed uniformly by TaskHandle
        // No need to join here, thread group will wait uniformly in TaskHandle

        // Clean up resources
        self.data_sender.take();
        self.data_receiver.take();
        self.control_sender.take();
        self.control_receiver.take();

        log::info!(
            "KafkaOutputSink closed: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- collect --------------------

    fn collect(&mut self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Print current state
        let state = self.state.lock().unwrap().clone();
        let data_sender_exists = self.data_sender.is_some();
        log::info!(
            "KafkaOutputSink collect: sink_id={}, topic={}, state={:?}, data_sender_exists={}",
            self.sink_id,
            self.config.topic,
            state,
            data_sender_exists
        );

        // Don't check state in main thread, send data directly to channel
        // If runloop is not running, data will be queued in channel
        if let Some(ref sender) = self.data_sender {
            sender
                .send(data)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        format!("Send failed: {}", e),
                    ))
                })?;
        }

        Ok(())
    }

    // -------------------- restore_state --------------------

    fn restore_state(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        log::info!(
            "Restoring state from checkpoint {} for topic: {}",
            checkpoint_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- flush --------------------

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // State check: if already closed, return error directly
        if matches!(*self.state.lock().unwrap(), ComponentState::Closed) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Flush aborted: component already closed",
            )));
        }

        // Send signal to runloop thread, let runloop thread handle it
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Flush {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send flush signal: {}",
                        e
                    )))
                })?;
        }

        // Wait with timeout retry for runloop thread to complete
        self.wait_with_retry(&completion_flag, "Flush")?;

        log::info!(
            "KafkaOutputSink flushed: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- box_clone --------------------

    fn box_clone(&self) -> Box<dyn OutputSink> {
        // Create a new KafkaOutputSink with the same config and sink_id
        // Note: cloned sink is uninitialized, needs to call init_with_context
        Box::new(KafkaOutputSink::new(self.config.clone(), self.sink_id))
    }
}
