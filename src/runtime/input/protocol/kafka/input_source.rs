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

// KafkaInputSource - Kafka input source implementation
//
// Implements InputSource that reads data from Kafka message queue
// Uses rdkafka client library for actual Kafka consumption
// Has an internal Kafka thread continuously consuming and putting messages into a fixed-length channel
// State changes are uniformly handled by the runloop thread (except init)

use super::config::KafkaConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::TaskCompletionFlag;
use crate::runtime::input::{InputSource, InputSourceState};
use crate::runtime::processor::function_error::FunctionErrorReport;
use crate::runtime::task::ControlMailBox;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ==================== Constants ====================

/// Default channel capacity (maximum number of messages in fixed-length channel)
const DEFAULT_CHANNEL_CAPACITY: usize = 10000;

/// Maximum number of messages for single batch consumption (to avoid continuous consumption preventing control signals from being processed)
const MAX_BATCH_CONSUME_SIZE: usize = 10000;

/// Control operation timeout (milliseconds)
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;

/// Maximum retry count for control operations
const CONTROL_OPERATION_MAX_RETRIES: u32 = 3;

// ==================== Enum Definitions ====================

/// Input source control signal (control layer)
///
/// Each signal contains a `completion_flag` to track whether the task has completed
#[derive(Debug, Clone)]
enum SourceControlSignal {
    /// Start signal
    Start { completion_flag: TaskCompletionFlag },
    /// Stop signal
    Stop { completion_flag: TaskCompletionFlag },
    /// Close signal
    Close { completion_flag: TaskCompletionFlag },
    /// Checkpoint start signal
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// Checkpoint finish signal
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    Error {
        completion_flag: TaskCompletionFlag,
        error: String,
    },
}

/// Control signal processing result
enum ControlAction {
    /// Continue running (process data)
    Continue,
    /// Pause (stop processing data, block waiting for control signal)
    Pause,
    /// Exit thread
    Exit,
}

// ==================== Struct Definitions ====================

/// KafkaInputSource - Kafka input source
///
/// Reads messages from Kafka topic and converts them to BufferOrEvent
///
/// Architecture:
/// - Has an internal Kafka consumer thread continuously consuming messages
/// - Consumed messages are put into a fixed-length channel
/// - Processor consumes data from the channel
/// - State changes are uniformly handled by the runloop thread (except init)
///
/// Note: Only cares about the byte array content of messages, does not parse internal structure of Kafka messages (topic, partition, offset, etc.)
pub struct KafkaInputSource {
    config: KafkaConfig,
    group_id: usize,
    input_id: usize,
    state: Arc<Mutex<InputSourceState>>,
    mail_box: Option<Arc<ControlMailBox>>,
    data_sender: Option<Sender<BufferOrEvent>>,
    data_receiver: Option<Receiver<BufferOrEvent>>,
    control_sender: Option<Sender<SourceControlSignal>>,
    control_receiver: Option<Receiver<SourceControlSignal>>,
    consumer_thread: Option<thread::JoinHandle<()>>,
}

impl KafkaInputSource {
    /// Create new Kafka input source from configuration
    ///
    /// # Arguments
    /// - `config`: Kafka configuration
    /// - `group_id`: Input group ID (starting from 0)
    /// - `input_id`: Input source ID within group (starting from 0, used to identify different input sources within the same group)
    pub fn from_config(config: KafkaConfig, group_id: usize, input_id: usize) -> Self {
        Self {
            config,
            group_id,
            input_id,
            state: Arc::new(Mutex::new(InputSourceState::Uninitialized)),
            mail_box: None,
            data_sender: None,
            data_receiver: None,
            control_sender: None,
            control_receiver: None,
            consumer_thread: None,
        }
    }

    // ==================== Timeout Retry Helper Functions ====================

    /// Wait for control operation completion with timeout retry
    ///
    /// Waits for completion_flag to mark completion and checks operation result
    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

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

    fn report_error(
        control_sender: Option<&Sender<SourceControlSignal>>,
        mail_box: Option<&Arc<ControlMailBox>>,
        input_id: usize,
        error: String,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let error_for_mailbox = error.clone();
        if let Some(control_sender) = control_sender {
            control_sender
                .send(SourceControlSignal::Error {
                    completion_flag: TaskCompletionFlag::new(),
                    error,
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send error signal: {}",
                        e
                    ))) as Box<dyn std::error::Error + Send>
                })?;
        }
        if let Some(m) = mail_box {
            m.send_error_report(FunctionErrorReport::input(input_id, error_for_mailbox))?;
        }
        Ok(())
    }

    // ==================== Consumer Thread Main Loop ====================

    /// Consumer thread main loop
    ///
    /// State machine driven event loop:
    /// - Running state: simultaneously waits for control signals and consumes Kafka messages
    /// - Paused state: only blocks waiting for control signals
    /// - All state changes are uniformly handled in this thread
    fn consumer_thread_loop(
        consumer: BaseConsumer,
        data_sender: Sender<BufferOrEvent>,
        control_receiver: Receiver<SourceControlSignal>,
        control_sender: Sender<SourceControlSignal>,
        state: Arc<Mutex<InputSourceState>>,
        mail_box: Option<Arc<ControlMailBox>>,
        input_id: usize,
        config: KafkaConfig,
    ) {
        use crossbeam_channel::select;

        // Initial state is paused, waiting for Start signal
        let mut is_running = false;
        log::debug!(
            "Consumer thread started (paused), waiting for start signal for topic: {} partition: {}",
            config.topic,
            config.partition_str()
        );

        loop {
            if is_running {
                // ========== Running state: simultaneously wait for control signals and consume Kafka messages ==========
                select! {
                    recv(control_receiver) -> result => {
                        match result {
                            Ok(signal) => {
                                match Self::handle_control_signal(signal, &consumer, &state, &config) {
                                    ControlAction::Continue => is_running = true,
                                    ControlAction::Pause => {
                                        is_running = false;
                                        log::info!("Source paused for topic: {} partition: {}", config.topic, config.partition_str());
                                    }
                                    ControlAction::Exit => break,
                                }
                            }
                            Err(_) => {
                                log::warn!("Control channel disconnected for topic: {} partition: {}", config.topic, config.partition_str());
                                break;
                            }
                        }
                    }
                    default() => {
                        Self::poll_and_send_messages(
                            &consumer,
                            &data_sender,
                            &control_sender,
                            mail_box.as_ref(),
                            input_id,
                            &config,
                        );
                    }
                }
            } else {
                // ========== Paused state: only block waiting for control signals ==========
                match control_receiver.recv() {
                    Ok(signal) => {
                        match Self::handle_control_signal(signal, &consumer, &state, &config) {
                            ControlAction::Continue => is_running = true,
                            ControlAction::Pause => is_running = false,
                            ControlAction::Exit => break,
                        }
                    }
                    Err(_) => {
                        log::warn!(
                            "Control channel disconnected for topic: {} partition: {}",
                            config.topic,
                            config.partition_str()
                        );
                        break;
                    }
                }
            }
        }

        // Don't commit offset, allow duplicate consumption
        log::info!(
            "Consumer thread exiting for topic: {} partition: {} (offset not committed)",
            config.topic,
            config.partition_str()
        );
    }

    // ==================== Control Layer Functions ====================

    fn handle_control_signal(
        signal: SourceControlSignal,
        consumer: &BaseConsumer,
        state: &Arc<Mutex<InputSourceState>>,
        config: &KafkaConfig,
    ) -> ControlAction {
        let current_state = state.lock().unwrap().clone();

        match signal {
            SourceControlSignal::Start { completion_flag } => {
                if !matches!(
                    current_state,
                    InputSourceState::Initialized
                        | InputSourceState::Stopped
                        | InputSourceState::Error { .. }
                ) {
                    let error = format!("Cannot start in state: {:?}", current_state);
                    log::error!(
                        "{} for topic: {} partition: {}",
                        error,
                        config.topic,
                        config.partition_str()
                    );
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                if let Err(e) =
                    consumer.fetch_metadata(Some(&config.topic), std::time::Duration::from_secs(5))
                {
                    let error = format!(
                        "Source fetch_metadata failed for topic {} partition {}: {}",
                        config.topic,
                        config.partition_str(),
                        e
                    );
                    log::error!("{}", error);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::debug!(
                    "Source start signal received for topic: {} partition: {}",
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SourceControlSignal::Stop { completion_flag } => {
                if !matches!(
                    current_state,
                    InputSourceState::Running | InputSourceState::Checkpointing
                ) {
                    let error = format!("Cannot stop in state: {:?}", current_state);
                    log::error!(
                        "{} for topic: {} partition: {}",
                        error,
                        config.topic,
                        config.partition_str()
                    );
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!(
                    "Source stop signal received for topic: {} partition: {}",
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }
            SourceControlSignal::Close { completion_flag } => {
                log::info!(
                    "Source close signal received for topic: {} partition: {}",
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Closing;
                *state.lock().unwrap() = InputSourceState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            SourceControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                if !matches!(current_state, InputSourceState::Running) {
                    let error = format!("Cannot take checkpoint in state: {:?}", current_state);
                    log::error!(
                        "{} for topic: {} partition: {}",
                        error,
                        config.topic,
                        config.partition_str()
                    );
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!(
                    "Checkpoint {} started for topic: {} partition: {}",
                    checkpoint_id,
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Checkpointing;
                log::info!(
                    "Checkpoint {}: Skipped offset commit (allow duplicate consumption)",
                    checkpoint_id
                );
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SourceControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                if !matches!(current_state, InputSourceState::Checkpointing) {
                    let error = format!("Cannot finish checkpoint in state: {:?}", current_state);
                    log::error!(
                        "{} for topic: {} partition: {}",
                        error,
                        config.topic,
                        config.partition_str()
                    );
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!(
                    "Checkpoint {} finish for topic: {} partition: {}",
                    checkpoint_id,
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SourceControlSignal::Error {
                completion_flag,
                error,
            } => {
                log::error!(
                    "Source error signal for topic: {} partition: {}",
                    config.topic,
                    config.partition_str()
                );
                *state.lock().unwrap() = InputSourceState::Error { error };
                completion_flag.mark_completed();
                ControlAction::Pause
            }
        }
    }

    /// Poll messages from Kafka and send to channel
    ///
    /// Runloop thread is responsible for constructing BufferOrEvent, main thread consumes directly
    /// Note: Does not commit offset, allows duplicate consumption
    fn poll_and_send_messages(
        consumer: &BaseConsumer,
        data_sender: &Sender<BufferOrEvent>,
        control_sender: &Sender<SourceControlSignal>,
        error_reporter: Option<&Arc<ControlMailBox>>,
        input_id: usize,
        config: &KafkaConfig,
    ) {
        let mut batch_count = 0;

        while batch_count < MAX_BATCH_CONSUME_SIZE {
            if data_sender.is_full() {
                break;
            }
            match consumer.poll(Duration::from_millis(1000)) {
                None => break,
                Some(Ok(message)) => {
                    if let Some(payload) = message.payload() {
                        let bytes = payload.to_vec();
                        let channel_info = Some(config.topic.clone());
                        let buffer_or_event =
                            BufferOrEvent::new_buffer(bytes, channel_info, false, false);

                        match data_sender.try_send(buffer_or_event) {
                            Ok(_) => {
                                batch_count += 1;
                                if data_sender.is_full() {
                                    break;
                                }
                            }
                            Err(crossbeam_channel::TrySendError::Full(_)) => break,
                            Err(crossbeam_channel::TrySendError::Disconnected(_)) => break,
                        }
                    }
                }
                Some(Err(e)) => {
                    let msg = e.to_string();
                    log::error!(
                        "Kafka poll error for topic {} partition {}: {}",
                        config.topic,
                        config.partition_str(),
                        msg
                    );
                    let _ = Self::report_error(Some(control_sender), error_reporter, input_id, msg);
                    break;
                }
            }
        }
    }

    // ==================== Configuration Validation ====================

    fn validate_kafka_config(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kafka bootstrap_servers is required",
            )));
        }

        if self.config.group_id.trim().is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kafka group_id is required",
            )));
        }

        if self.config.topic.trim().is_empty() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kafka topic is required",
            )));
        }

        if let Some(partition) = self.config.partition
            && partition < 0
        {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Kafka partition must be >= 0, got: {}", partition),
            )));
        }

        // Validate enable.auto.commit must be false
        if let Some(auto_commit) = self.config.properties.get("enable.auto.commit")
            && auto_commit.to_lowercase().trim() == "true"
        {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "enable.auto.commit must be false for manual offset commit",
            )));
        }

        Ok(())
    }

    fn create_consumer(&self) -> Result<BaseConsumer, Box<dyn std::error::Error + Send>> {
        self.validate_kafka_config()?;

        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", self.config.bootstrap_servers_str());
        client_config.set("group.id", &self.config.group_id);
        client_config.set("enable.partition.eof", "false");
        client_config.set("enable.auto.commit", "false");

        for (key, value) in &self.config.properties {
            if key != "enable.auto.commit" {
                client_config.set(key, value);
            }
        }

        let consumer: BaseConsumer = client_config.create().map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Failed to create Kafka consumer: {}",
                e
            ))) as Box<dyn std::error::Error + Send>
        })?;

        // Subscribe to topic or assign specific partition
        if let Some(partition) = self.config.partition {
            // Partition specified, use assign
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&self.config.topic, partition);
            consumer.assign(&tpl).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to assign partition {}: {}",
                    partition, e
                ))) as Box<dyn std::error::Error + Send>
            })?;
        } else {
            // Partition not specified, use subscribe auto-assignment
            consumer.subscribe(&[&self.config.topic]).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to subscribe to topic '{}': {}",
                    self.config.topic, e
                ))) as Box<dyn std::error::Error + Send>
            })?;
        }

        match consumer.fetch_metadata(Some(&self.config.topic), std::time::Duration::from_secs(5)) {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(std::io::Error::other(format!(
                    "Connection validation failed: {}",
                    e
                ))));
            }
        }

        Ok(consumer)
    }
}

// ==================== InputSource Trait Implementation ====================

impl InputSource for KafkaInputSource {
    fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // init_with_context is the only method that sets state in the caller thread (because runloop thread hasn't started yet)
        if !matches!(*self.state.lock().unwrap(), InputSourceState::Uninitialized) {
            return Ok(());
        }

        let mailbox_handle = init_context.get_control_mailbox();
        if let Some(ref m) = *mailbox_handle.lock().unwrap() {
            self.mail_box = Some(Arc::clone(m));
        }

        self.validate_kafka_config()?;

        // Create Channel
        let (data_sender, data_receiver) = bounded(DEFAULT_CHANNEL_CAPACITY);
        let (control_sender, control_receiver) = unbounded();

        self.data_sender = Some(data_sender.clone());
        self.data_receiver = Some(data_receiver);
        self.control_sender = Some(control_sender.clone());
        self.control_receiver = Some(control_receiver);

        // Create Kafka consumer and start thread
        let consumer = self.create_consumer()?;
        let config_clone = self.config.clone();
        let state_clone = self.state.clone();

        // Take control_receiver from struct for thread use
        let control_receiver_for_thread = self.control_receiver.take().ok_or_else(|| {
            Box::new(std::io::Error::other("control_receiver is None"))
                as Box<dyn std::error::Error + Send>
        })?;

        let error_reporter_clone = self.mail_box.clone();
        let input_id = self.input_id;

        let thread_name = format!(
            "kafka-source-g{}-i{}-{}-{}",
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        let thread_handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                Self::consumer_thread_loop(
                    consumer,
                    data_sender,
                    control_receiver_for_thread,
                    control_sender,
                    state_clone,
                    error_reporter_clone,
                    input_id,
                    config_clone,
                );
            })
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to start thread: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })?;

        // Register thread group to InitContext
        use crate::runtime::processor::wasm::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut input_thread_group = ThreadGroup::new(
            ThreadGroupType::InputSource(self.group_id),
            format!("InputSource-g{}-i{}", self.group_id, self.input_id),
        );
        input_thread_group.add_thread(thread_handle);
        init_context.register_thread_group(input_thread_group);

        // Note: Thread handle has been moved to thread group, no longer stored in consumer_thread
        // When closing, need to manage thread through TaskHandle
        self.consumer_thread = None;
        *self.state.lock().unwrap() = InputSourceState::Initialized;
        Ok(())
    }

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, handled by runloop thread's handle_control_signal
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SourceControlSignal::Start {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send start signal: {}",
                        e
                    ))) as Box<dyn std::error::Error + Send>
                })?;
        }

        self.wait_with_retry(&completion_flag, "Start")?;

        log::debug!(
            "KafkaInputSource started: group_id={}, input_id={}, topic={}, partition={}",
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, handled by runloop thread's handle_control_signal
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SourceControlSignal::Stop {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to send stop signal: {}",
                        e
                    ))) as Box<dyn std::error::Error + Send>
                })?;
        }

        self.wait_with_retry(&completion_flag, "Stop")?;

        log::info!(
            "KafkaInputSource stopped: group_id={}, input_id={}, topic={}, partition={}",
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        Ok(())
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        if matches!(*self.state.lock().unwrap(), InputSourceState::Closed) {
            return Ok(());
        }

        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SourceControlSignal::Close {
                completion_flag: completion_flag.clone(),
            };
            if control_sender.send(signal).is_ok() {
                let _ = self.wait_with_retry(&completion_flag, "Close");
            }
        }

        // Note: Thread handle has been moved to thread group, uniformly managed by TaskHandle
        // No need to join here, thread group will wait uniformly in TaskHandle

        // Clean up resources
        self.data_sender.take();
        self.data_receiver.take();
        self.control_sender.take();
        self.control_receiver.take();

        log::info!(
            "KafkaInputSource closed: group_id={}, input_id={}, topic={}, partition={}",
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        Ok(())
    }

    fn get_next(&mut self) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        if let Some(ref receiver) = self.data_receiver {
            match receiver.try_recv() {
                Ok(buffer_or_event) => Ok(Some(buffer_or_event)),
                Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
                Err(crossbeam_channel::TryRecvError::Disconnected) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, handled by runloop thread's handle_control_signal
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SourceControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender.send(signal).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Checkpoint signal failed: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })?;
        }

        self.wait_with_retry(&completion_flag, "Checkpoint")?;

        log::info!(
            "Checkpoint {} started: group_id={}, input_id={}, topic={}, partition={}",
            checkpoint_id,
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        Ok(())
    }

    fn finish_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Don't check state in main thread, handled by runloop thread's handle_control_signal
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SourceControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender.send(signal).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to send checkpoint finish signal: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })?;
        }

        self.wait_with_retry(&completion_flag, "CheckpointFinish")?;

        log::info!(
            "Checkpoint {} finished: group_id={}, input_id={}, topic={}, partition={}",
            checkpoint_id,
            self.group_id,
            self.input_id,
            self.config.topic,
            self.config.partition_str()
        );
        Ok(())
    }

    fn get_group_id(&self) -> usize {
        self.group_id
    }

    fn set_error_state(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SourceControlSignal::Error {
                    completion_flag: completion_flag.clone(),
                    error: String::new(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to set error state: {}",
                        e
                    ))) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "SetErrorState")
    }
}
