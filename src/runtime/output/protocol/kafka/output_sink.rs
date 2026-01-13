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

// KafkaOutputSink - Kafka 输出接收器实现
//
// 实现向 Kafka 消息队列发送数据的 OutputSink
// 使用 rdkafka 客户端库进行实际的 Kafka 生产

use super::producer_config::KafkaProducerConfig;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::output::OutputSink;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use std::sync::Arc;
use std::sync::Mutex;

// ==================== 常量 ====================

/// 默认管道容量（定长管道的最大消息数）
const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

/// 单次批量消费的最大消息数（避免一直消费导致控制信号得不到处理）
const MAX_BATCH_CONSUME_SIZE: usize = 100;

/// 默认 flush 超时时间（毫秒）
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 5000;

/// 控制操作超时时间（毫秒）
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;

/// 控制操作最大重试次数
const CONTROL_OPERATION_MAX_RETRIES: u32 = 5;

// ==================== 枚举定义 ====================

/// 接收器控制信号（控制层）
///
/// 每个信号都包含一个 `completion_flag`，用于追踪任务是否已完成
#[derive(Debug, Clone)]
enum SinkControlSignal {
    /// 启动信号
    Start { completion_flag: TaskCompletionFlag },
    /// 停止信号
    Stop { completion_flag: TaskCompletionFlag },
    /// 关闭信号
    Close { completion_flag: TaskCompletionFlag },
    /// 开始检查点信号
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// 结束检查点信号
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// 刷新信号
    Flush { completion_flag: TaskCompletionFlag },
}

/// 控制信号处理结果
enum ControlAction {
    /// 继续运行（处理数据）
    Continue,
    /// 暂停（停止处理数据，阻塞等待控制信号）
    Pause,
    /// 退出线程
    Exit,
}

// ==================== 结构体定义 ====================

/// KafkaOutputSink - Kafka 输出接收器
///
/// 使用独立的线程处理数据发送，内部有数据缓存 Channel
/// 架构：
/// - 主线程将数据放入 Channel
/// - 发送线程从 Channel 消费数据并发送到 Kafka
/// - 支持控制信号（停止、关闭、检查点）
/// - 状态变更统一由 runloop 线程处理（除了 init）
pub struct KafkaOutputSink {
    /// Kafka 配置
    config: KafkaProducerConfig,
    /// 输出接收器 ID（从 0 开始，用于标识不同的输出接收器）
    sink_id: usize,
    /// 组件状态（共享，由 runloop 线程统一管理）
    state: Arc<Mutex<ComponentState>>,
    /// 数据发送线程
    send_thread: Option<std::thread::JoinHandle<()>>,
    /// 数据缓存 Channel 发送端（主线程写入）
    data_sender: Option<crossbeam_channel::Sender<BufferOrEvent>>,
    /// 数据缓存 Channel 接收端（发送线程读取）
    data_receiver: Option<crossbeam_channel::Receiver<BufferOrEvent>>,
    /// 控制信号 Channel 发送端
    control_sender: Option<crossbeam_channel::Sender<SinkControlSignal>>,
    /// 控制信号 Channel 接收端
    control_receiver: Option<crossbeam_channel::Receiver<SinkControlSignal>>,
}

impl KafkaOutputSink {
    // ==================== 配置/构造 ====================

    /// 创建新的 Kafka 输出接收器
    ///
    /// # 参数
    /// - `config`: Kafka 配置
    /// - `sink_id`: 输出接收器 ID（从 0 开始，用于标识不同的输出接收器）
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

    /// 从配置创建
    ///
    /// # 参数
    /// - `config`: Kafka 配置
    /// - `sink_id`: 输出接收器 ID（从 0 开始，用于标识不同的输出接收器）
    pub fn from_config(config: KafkaProducerConfig, sink_id: usize) -> Self {
        Self::new(config, sink_id)
    }

    // ==================== 超时重试辅助函数 ====================

    /// 带超时重试的等待控制操作完成
    ///
    /// 等待 completion_flag 标记完成，并检查操作结果
    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = std::time::Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    // 检查操作结果
                    if let Some(error) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(
                            format!("{} failed: {}", operation_name, error),
                        )));
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

    /// flush 专用的超时重试等待，检查操作结果
    fn wait_flush_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = std::time::Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    // 检查操作结果
                    if let Some(error) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(
                            format!("Flush failed: {}", error),
                        )));
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

    // ==================== 发送线程主循环 ====================

    /// 发送线程主循环
    ///
    /// 状态机驱动的事件循环：
    /// - 运行状态：同时等待控制信号和数据
    /// - 暂停状态：只阻塞等待控制信号
    /// - 所有状态变更统一在此线程处理
    fn send_thread_loop(
        producer: ThreadedProducer<DefaultProducerContext>,
        data_receiver: crossbeam_channel::Receiver<BufferOrEvent>,
        control_receiver: crossbeam_channel::Receiver<SinkControlSignal>,
        state: Arc<Mutex<ComponentState>>,
        config: KafkaProducerConfig,
    ) {
        use crossbeam_channel::select;

        // 初始状态为暂停，等待 Start 信号
        let mut is_running = false;
        log::info!(
            "Send thread started (paused), waiting for start signal for topic: {}",
            config.topic
        );

        loop {
            if is_running {
                // ========== 运行状态：同时等待控制信号和数据 ==========
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
                                // 发送当前消息
                                Self::send_message(&producer, data, &config);

                                // 非阻塞地消费并发送更多数据，减少 select! 调度开销
                                // 限制批次大小，确保控制信号能及时处理
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

                                // 批量结束后 flush，确保消息发送到 Kafka
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
                // ========== 暂停状态：只阻塞等待控制信号 ==========
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

    // ==================== 控制层函数 ====================

    /// 处理控制信号（在 runloop 线程中执行，统一管理状态变更和状态检查）
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
                // 只有 Initialized 或 Stopped 状态可以 Start
                if !matches!(
                    current_state,
                    ComponentState::Initialized | ComponentState::Stopped
                ) {
                    let error = format!("Cannot start in state: {:?}", current_state);
                    log::error!("{} for topic: {}", error, config.topic);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }
                log::info!("Sink start signal received for topic: {}", config.topic);
                *state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            SinkControlSignal::Stop { completion_flag } => {
                // 只有 Running 或 Checkpointing 状态可以 Stop
                if !matches!(
                    current_state,
                    ComponentState::Running | ComponentState::Checkpointing
                ) {
                    // Stop 操作如果状态不对，静默成功（幂等）
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
                // Close 可以在任何状态下执行
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
                // 只有 Running 状态可以 Checkpoint
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
                // 只有 Checkpointing 状态可以 CheckpointFinish
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

    /// 在关闭时，处理 data channel 中所有剩余的数据，防止泄漏
    fn drain_remaining_data(
        producer: &ThreadedProducer<DefaultProducerContext>,
        data_receiver: &crossbeam_channel::Receiver<BufferOrEvent>,
        config: &KafkaProducerConfig,
    ) {
        let mut drained_count = 0;

        // 非阻塞地取出并发送所有剩余数据
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

    /// 刷新 Kafka Producer
    fn flush_producer(producer: &ThreadedProducer<DefaultProducerContext>) {
        let _ = producer.flush(std::time::Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS));
    }

    // ==================== 数据层函数 ====================

    /// 发送单条消息
    ///
    /// ThreadedProducer 内部会自动批量处理，无需手动批量
    #[inline]
    fn send_message(
        producer: &ThreadedProducer<DefaultProducerContext>,
        data: BufferOrEvent,
        config: &KafkaProducerConfig,
    ) {
        // 使用 into_buffer() 获取所有权，避免额外复制
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

// ==================== OutputSink Trait 实现 ====================

impl OutputSink for KafkaOutputSink {
    // -------------------- init --------------------

    fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // init_with_context 是唯一在调用者线程设置状态的方法（因为此时 runloop 线程还未启动）
        if !matches!(*self.state.lock().unwrap(), ComponentState::Uninitialized) {
            return Ok(());
        }

        // 验证配置
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

        // 创建 Channel
        let (data_sender, data_receiver) = crossbeam_channel::bounded(DEFAULT_CHANNEL_CAPACITY);
        let (control_sender, control_receiver) = crossbeam_channel::bounded(10);
        self.data_sender = Some(data_sender);
        self.control_sender = Some(control_sender);

        // 创建 Kafka 生产者并启动线程
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
                Box::new(std::io::Error::other(
                    format!("Failed to start thread: {}", e),
                ))
            })?;

        // 注册线程组到 InitContext
        use crate::runtime::processor::WASM::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut output_thread_group = ThreadGroup::new(
            ThreadGroupType::OutputSink(self.sink_id),
            format!("OutputSink-{}", self.sink_id),
        );
        output_thread_group.add_thread(thread_handle, thread_name);
        init_context.register_thread_group(output_thread_group);

        // 注意：线程句柄已经移动到线程组中，不再存储在 send_thread 中
        // 在 close 时，需要通过 TaskHandle 来管理线程
        self.send_thread = None;
        // init_with_context 是唯一在调用者线程设置状态的地方
        *self.state.lock().unwrap() = ComponentState::Initialized;
        log::info!(
            "KafkaOutputSink initialized: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- start --------------------

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 不在主线程判断状态，由 runloop 线程的 handle_control_signal 处理
        // 发送信号给 runloop 线程，由 runloop 线程设置状态
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Start {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(
                        format!("Failed to send start signal: {}", e),
                    ))
                })?;
        }

        // 带超时重试等待 runloop 线程处理完成
        self.wait_with_retry(&completion_flag, "Start")?;

        log::info!(
            "KafkaOutputSink started: sink_id={}, topic={}",
            self.sink_id,
            self.config.topic
        );
        Ok(())
    }

    // -------------------- stop --------------------

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 不在主线程判断状态，由 runloop 线程的 handle_control_signal 处理
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Stop {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(
                        format!("Failed to send stop signal: {}", e),
                    ))
                })?;
        }

        // 带超时重试等待 runloop 线程处理完成
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
        // 不在主线程判断状态，由 runloop 线程的 handle_control_signal 处理
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender
                .send(signal)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(
                        format!("Checkpoint signal failed: {}", e),
                    ))
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
        // 不在主线程判断状态，由 runloop 线程的 handle_control_signal 处理
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag: completion_flag.clone(),
            };
            control_sender
                .send(signal)
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(
                        format!("Failed to send checkpoint finish signal: {}", e),
                    ))
                })?;
        }

        // 带超时重试等待 runloop 线程处理完成
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
        // 状态检查（只读）
        if matches!(*self.state.lock().unwrap(), ComponentState::Closed) {
            return Ok(());
        }

        // 发送信号给 runloop 线程，由 runloop 线程设置状态
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            let signal = SinkControlSignal::Close {
                completion_flag: completion_flag.clone(),
            };
            if control_sender.send(signal).is_ok() {
                // 带超时重试等待 runloop 线程处理完成
                // Close 操作允许失败（可能线程已退出），所以忽略错误
                let _ = self.wait_with_retry(&completion_flag, "Close");
            }
        }

        // 注意：线程句柄已经移动到线程组中，由 TaskHandle 统一管理
        // 这里不再需要 join，线程组会在 TaskHandle 中统一等待

        // 清理资源
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
        // 打印当前状态
        let state = self.state.lock().unwrap().clone();
        let data_sender_exists = self.data_sender.is_some();
        log::info!(
            "KafkaOutputSink collect: sink_id={}, topic={}, state={:?}, data_sender_exists={}",
            self.sink_id,
            self.config.topic,
            state,
            data_sender_exists
        );

        // 不在主线程判断状态，直接发送数据到 channel
        // 如果 runloop 没有在运行，数据会被积压在 channel 中
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
        // 状态检查：如果已关闭，直接返回错误
        if matches!(*self.state.lock().unwrap(), ComponentState::Closed) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Flush aborted: component already closed",
            )));
        }

        // 发送信号给 runloop 线程，由 runloop 线程处理
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref control_sender) = self.control_sender {
            control_sender
                .send(SinkControlSignal::Flush {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send> {
                    Box::new(std::io::Error::other(
                        format!("Failed to send flush signal: {}", e),
                    ))
                })?;
        }

        // 带超时重试等待 runloop 线程处理完成
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
        // 创建一个新的 KafkaOutputSink，使用相同的配置和 sink_id
        // 注意：克隆的 sink 是未初始化的，需要调用 init_with_context
        Box::new(KafkaOutputSink::new(self.config.clone(), self.sink_id))
    }
}
