// WasmTask - WASM 任务实现
//
// 架构设计：
// - 专门的 runloop 线程持有所有数据（inputs, processor, sinks），不需要锁
// - 控制信号通过 channel 传递，不用原子变量
// - 状态由 runloop 线程统一管理
// - 热点路径（process_input）没有锁和原子变量操作

use super::thread_pool::{TaskThreadPool, ThreadGroup};
use super::wasm_processor_trait::WasmProcessor;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::input::InputSource;
use crate::runtime::output::OutputSink;
use crate::runtime::task::TaskLifecycle;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// 控制操作超时（毫秒）
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;
/// 最大重试次数
const CONTROL_OPERATION_MAX_RETRIES: u32 = 3;
/// 数据处理批次大小
const MAX_BATCH_SIZE: usize = 100;

/// 流任务环境配置
#[derive(Clone, Debug)]
pub struct TaskEnvironment {
    pub task_name: String,
}

impl TaskEnvironment {
    pub fn new(task_name: String) -> Self {
        Self { task_name }
    }
}

/// 任务控制信号
enum TaskControlSignal {
    /// 启动任务
    Start { completion_flag: TaskCompletionFlag },
    /// 停止任务
    Stop { completion_flag: TaskCompletionFlag },
    /// 取消任务
    Cancel { completion_flag: TaskCompletionFlag },
    /// 开始检查点
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// 完成检查点
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    /// 关闭任务
    Close { completion_flag: TaskCompletionFlag },
}

/// runloop 线程的控制动作
enum ControlAction {
    Continue,
    Pause,
    Exit,
}

/// 任务状态（仅在 runloop 线程内使用，不需要同步）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskState {
    Uninitialized,
    Initialized,
    Running,
    Stopped,
    Checkpointing,
    Closing,
    Closed,
    Failed,
}

/// 执行状态（用于线程管理）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    /// 任务已创建
    Created,
    /// 正在部署
    Deploying,
    /// 正在初始化
    Initializing,
    /// 正在运行
    Running,
    /// 已完成
    Finished,
    /// 正在取消
    Canceling,
    /// 已取消
    Canceled,
    /// 已失败
    Failed,
}

impl ExecutionState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => ExecutionState::Created,
            1 => ExecutionState::Deploying,
            2 => ExecutionState::Initializing,
            3 => ExecutionState::Running,
            4 => ExecutionState::Finished,
            5 => ExecutionState::Canceling,
            6 => ExecutionState::Canceled,
            7 => ExecutionState::Failed,
            _ => ExecutionState::Created,
        }
    }
}

pub struct WasmTask {
    /// 任务名称
    task_name: String,
    /// 环境配置
    environment: TaskEnvironment,
    /// 输入源列表（在 init 后移动到线程中）
    inputs: Option<Vec<Box<dyn InputSource>>>,
    /// 处理器（在 init 后移动到线程中）
    processor: Option<Box<dyn WasmProcessor>>,
    /// 输出接收器列表（在 init 后移动到线程中）
    sinks: Option<Vec<Box<dyn OutputSink>>>,
    /// 共享状态（用于外部查询，runloop 线程更新）
    state: Arc<Mutex<ComponentState>>,
    /// 控制信号发送端
    control_sender: Option<Sender<TaskControlSignal>>,
    /// runloop 线程句柄
    task_thread: Option<JoinHandle<()>>,
    /// 线程池引用
    thread_pool: Option<Arc<TaskThreadPool>>,
    /// 线程组信息（在 init_with_context 中收集）
    thread_groups: Option<Vec<ThreadGroup>>,
    /// 执行状态（用于线程管理）
    execution_state: Arc<AtomicU8>,
    /// 失败原因
    failure_cause: Arc<Mutex<Option<String>>>,
    /// 线程是否还在运行的标志（通过原子变量跟踪）
    thread_running: Arc<AtomicBool>,
    /// 终止 Future（用于等待任务完成）
    termination_future: Arc<Mutex<Option<mpsc::Receiver<ExecutionState>>>>,
}

impl WasmTask {
    /// 创建新的流任务
    ///
    /// # 参数
    /// - `environment`: 任务环境配置
    /// - `inputs`: 输入源列表
    /// - `processor`: 处理器
    /// - `sinks`: 输出接收器列表
    ///
    /// # 返回值
    /// - `Ok(WasmTask)`: 成功创建的任务
    /// - `Err(...)`: 创建失败
    pub fn new(
        environment: TaskEnvironment,
        inputs: Vec<Box<dyn InputSource>>,
        processor: Box<dyn WasmProcessor>,
        sinks: Vec<Box<dyn OutputSink>>,
    ) -> Self {
        let (_tx, rx) = mpsc::channel();
        Self {
            task_name: environment.task_name.clone(),
            environment,
            inputs: Some(inputs),
            processor: Some(processor),
            sinks: Some(sinks),
            state: Arc::new(Mutex::new(ComponentState::Uninitialized)),
            control_sender: None,
            task_thread: None,
            thread_pool: None,
            thread_groups: None,
            execution_state: Arc::new(AtomicU8::new(ExecutionState::Created as u8)),
            failure_cause: Arc::new(Mutex::new(None)),
            thread_running: Arc::new(AtomicBool::new(false)),
            termination_future: Arc::new(Mutex::new(Some(rx))),
        }
    }

    /// 初始化任务（启动 runloop 线程）
    ///
    /// # 参数
    /// - `init_context`: 初始化上下文（包含线程池）
    pub fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let total_start = std::time::Instant::now();

        // 从结构体中取出，准备移动到线程中
        // 注意：这些字段在移动到线程后，结构体中会变为 None，但结构体本身仍然存在
        // 由于这些字段在 init 后不再被访问，直接移动即可
        let take_start = std::time::Instant::now();
        let mut inputs = self.inputs.take().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "inputs already moved to thread",
            )) as Box<dyn std::error::Error + Send>
        })?;
        let mut processor = self.processor.take().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "processor already moved to thread",
            )) as Box<dyn std::error::Error + Send>
        })?;
        let mut sinks = self.sinks.take().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "sinks already moved to thread",
            )) as Box<dyn std::error::Error + Send>
        })?;
        let take_elapsed = take_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Take fields: {:.3}s",
            take_elapsed
        );

        // 克隆 init_context 以便在闭包中使用
        let clone_start = std::time::Instant::now();
        let init_context = init_context.clone();
        let clone_elapsed = clone_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Clone context: {:.3}s",
            clone_elapsed
        );

        // 初始化顺序：先 Sink，再 Processor，最后 Source
        // 这样 Processor 在初始化时可以使用 Sinks 来创建 WasmHost

        // 1. 先初始化所有 sinks
        let sinks_init_start = std::time::Instant::now();
        for (idx, sink) in sinks.iter_mut().enumerate() {
            let sink_start = std::time::Instant::now();
            if let Err(e) = sink.init_with_context(&init_context) {
                log::error!("Failed to init sink {}: {}", idx, e);
                return Err(Box::new(std::io::Error::other(
                    format!("Failed to init sink {}: {}", idx, e),
                )));
            }
            let sink_elapsed = sink_start.elapsed().as_secs_f64();
            log::info!(
                "[Timing] init_with_context - Init sink {}: {:.3}s",
                idx,
                sink_elapsed
            );
        }
        let sinks_init_elapsed = sinks_init_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Init all sinks: {:.3}s",
            sinks_init_elapsed
        );

        // 2. 初始化 processor
        let processor_init_start = std::time::Instant::now();
        if let Err(e) = processor.init_with_context(&init_context) {
            log::error!("Failed to init processor: {}", e);
            return Err(Box::new(std::io::Error::other(
                format!("Failed to init processor: {}", e),
            )));
        }
        let processor_init_elapsed = processor_init_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Init processor: {:.3}s",
            processor_init_elapsed
        );

        // 2.1. 初始化 WasmHost（需要传入 sinks）
        // 直接将 sinks 的所有权传递给 WasmHost（不克隆）
        use crate::runtime::processor::WASM::WasmProcessorImpl;
        let wasm_host_init_start = std::time::Instant::now();
        if let Some(wasm_processor_impl) =
            processor.as_any_mut().downcast_mut::<WasmProcessorImpl>()
        {
            if let Err(e) =
                wasm_processor_impl.init_wasm_host(sinks, &init_context, self.task_name.clone())
            {
                log::error!("Failed to init WasmHost: {}", e);
                return Err(Box::new(std::io::Error::other(
                    format!("Failed to init WasmHost: {}", e),
                )));
            }
        } else {
            return Err(Box::new(std::io::Error::other(
                "Processor is not a WasmProcessorImpl, cannot initialize WasmHost",
            )));
        }
        let wasm_host_init_elapsed = wasm_host_init_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Init WasmHost: {:.3}s",
            wasm_host_init_elapsed
        );

        // 注意：sinks 的所有权已经移交给 WasmHost（在 Store<HostState> 中）
        // runloop 线程不再需要 sinks，因为输出通过 WASM processor 内部的 collector 完成

        // 3. 最后初始化所有 input sources
        let inputs_init_start = std::time::Instant::now();
        for (idx, input) in inputs.iter_mut().enumerate() {
            let input_start = std::time::Instant::now();
            if let Err(e) = input.init_with_context(&init_context) {
                log::error!("Failed to init input {}: {}", idx, e);
                return Err(Box::new(std::io::Error::other(
                    format!("Failed to init input {}: {}", idx, e),
                )));
            }
            let input_elapsed = input_start.elapsed().as_secs_f64();
            log::info!(
                "[Timing] init_with_context - Init input {}: {:.3}s",
                idx,
                input_elapsed
            );
        }
        let inputs_init_elapsed = inputs_init_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Init all inputs: {:.3}s",
            inputs_init_elapsed
        );

        // 创建控制 channel
        let channel_start = std::time::Instant::now();
        let (control_sender, control_receiver) = bounded(10);
        self.control_sender = Some(control_sender);

        let task_name = self.task_name.clone();
        let state = self.state.clone();
        let execution_state = self.execution_state.clone();
        let thread_running = self.thread_running.clone();
        let termination_tx = {
            let (_tx, rx) = mpsc::channel();
            *self.termination_future.lock().unwrap() = Some(rx);
            _tx
        };
        let channel_elapsed = channel_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Create channels: {:.3}s",
            channel_elapsed
        );

        // 标记线程开始运行
        let thread_prep_start = std::time::Instant::now();
        thread_running.store(true, Ordering::Relaxed);
        self.execution_state
            .store(ExecutionState::Initializing as u8, Ordering::Relaxed);
        let thread_prep_elapsed = thread_prep_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Prepare thread state: {:.3}s",
            thread_prep_elapsed
        );

        // 启动 runloop 线程，将所有数据 move 进去
        let thread_spawn_start = std::time::Instant::now();
        let thread_handle = thread::Builder::new()
            .name(format!("stream-task-{}", task_name))
            .spawn(move || {
                Self::task_thread_loop(task_name, inputs, processor, control_receiver, state);

                // 线程结束时更新状态
                execution_state.store(ExecutionState::Finished as u8, Ordering::Relaxed);
                thread_running.store(false, Ordering::Relaxed);
                let _ = termination_tx.send(ExecutionState::Finished);
            })
            .map_err(|e| {
                Box::new(std::io::Error::other(
                    format!("Failed to start task thread: {}", e),
                )) as Box<dyn std::error::Error + Send>
            })?;
        let thread_spawn_elapsed = thread_spawn_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Spawn thread: {:.3}s",
            thread_spawn_elapsed
        );

        // 注册主 runloop 线程组
        let register_start = std::time::Instant::now();
        use crate::runtime::processor::WASM::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut main_runloop_group = ThreadGroup::new(
            ThreadGroupType::MainRunloop,
            format!("MainRunloop-{}", self.task_name),
        );
        main_runloop_group.add_thread(thread_handle, format!("stream-task-{}", self.task_name));
        init_context.register_thread_group(main_runloop_group);
        let register_elapsed = register_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Register thread group: {:.3}s",
            register_elapsed
        );

        let total_elapsed = total_start.elapsed().as_secs_f64();
        log::info!("[Timing] init_with_context total: {:.3}s", total_elapsed);

        // 从注册器中获取所有线程组（包括 inputs、processor、sinks、main runloop）
        let take_groups_start = std::time::Instant::now();
        let thread_groups = init_context.take_thread_groups();
        let take_groups_elapsed = take_groups_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Take thread groups: {:.3}s",
            take_groups_elapsed
        );
        log::info!(
            "WasmTask '{}' registered {} thread groups",
            self.task_name,
            thread_groups.len()
        );

        // 存储线程组信息，以便在 TaskThreadPool::submit 中使用
        let store_groups_start = std::time::Instant::now();
        self.thread_groups = Some(thread_groups);
        let store_groups_elapsed = store_groups_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Store thread groups: {:.3}s",
            store_groups_elapsed
        );

        let finalize_start = std::time::Instant::now();
        self.task_thread = None; // 线程句柄已经移动到线程组中
        self.execution_state
            .store(ExecutionState::Running as u8, Ordering::Relaxed);
        let finalize_elapsed = finalize_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] init_with_context - Finalize state: {:.3}s",
            finalize_elapsed
        );

        let total_elapsed = total_start.elapsed().as_secs_f64();
        log::info!(
            "WasmTask initialized: {} (total init_with_context: {:.3}s)",
            self.task_name,
            total_elapsed
        );
        Ok(())
    }

    // ==================== runloop 线程主循环 ====================

    fn task_thread_loop(
        task_name: String,
        mut inputs: Vec<Box<dyn InputSource>>,
        mut processor: Box<dyn WasmProcessor>,
        control_receiver: Receiver<TaskControlSignal>,
        shared_state: Arc<Mutex<ComponentState>>,
    ) {
        let thread_start_time = std::time::Instant::now();
        use crossbeam_channel::select;

        // 本地状态，无需同步
        let init_start = std::time::Instant::now();
        let mut state = TaskState::Initialized;
        let mut current_input_index: usize = 0;
        let mut is_running = false;
        let init_elapsed = init_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] task_thread_loop - Initialize local state: {:.3}s",
            init_elapsed
        );

        // 更新共享状态
        let lock_start = std::time::Instant::now();
        *shared_state.lock().unwrap() = ComponentState::Initialized;
        let lock_elapsed = lock_start.elapsed().as_secs_f64();
        log::info!(
            "[Timing] task_thread_loop - Update shared state: {:.3}s",
            lock_elapsed
        );

        let thread_init_elapsed = thread_start_time.elapsed().as_secs_f64();
        log::info!(
            "Task thread started (paused): {} (thread init: {:.3}s)",
            task_name,
            thread_init_elapsed
        );

        loop {
            if is_running {
                // ========== 运行状态：优先处理控制信号，然后处理数据 ==========
                select! {
                    recv(control_receiver) -> result => {
                        match result {
                            Ok(signal) => {
                                match Self::handle_control_signal(
                                    signal,
                                    &mut state,
                                    &mut inputs,
                                    &mut processor,
                                    &shared_state,
                                    &task_name,
                                ) {
                                    ControlAction::Continue => is_running = true,
                                    ControlAction::Pause => is_running = false,
                                    ControlAction::Exit => break,
                                }
                            }
                            Err(_) => {
                                log::warn!("Control channel disconnected: {}", task_name);
                                break;
                            }
                        }
                    }
                    default => {
                        // 没有控制信号，处理数据
                        Self::process_batch(
                            &mut inputs,
                            &mut processor,
                            &mut current_input_index,
                        );
                    }
                }
            } else {
                // ========== 暂停状态：只阻塞等待控制信号 ==========
                match control_receiver.recv() {
                    Ok(signal) => {
                        match Self::handle_control_signal(
                            signal,
                            &mut state,
                            &mut inputs,
                            &mut processor,
                            &shared_state,
                            &task_name,
                        ) {
                            ControlAction::Continue => is_running = true,
                            ControlAction::Pause => is_running = false,
                            ControlAction::Exit => break,
                        }
                    }
                    Err(_) => {
                        log::warn!("Control channel disconnected: {}", task_name);
                        break;
                    }
                }
            }
        }

        // 线程退出，清理资源
        Self::cleanup_resources(&mut inputs, &mut processor, &task_name);
        log::info!("Task thread exiting: {}", task_name);
    }

    // ==================== 控制信号处理 ====================

    /// 处理控制信号（在 runloop 线程中执行）
    fn handle_control_signal(
        signal: TaskControlSignal,
        state: &mut TaskState,
        inputs: &mut Vec<Box<dyn InputSource>>,
        processor: &mut Box<dyn WasmProcessor>,
        shared_state: &Arc<Mutex<ComponentState>>,
        task_name: &str,
    ) -> ControlAction {
        match signal {
            TaskControlSignal::Start { completion_flag } => {
                if *state != TaskState::Initialized && *state != TaskState::Stopped {
                    let error = format!("Cannot start in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Pause;
                }

                log::info!("Starting task: {}", task_name);

                // 启动所有输入源（组件已经在 init_with_context 中初始化过了）
                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.start() {
                        log::error!("Failed to start input {}: {}", idx, e);
                    }
                }

                // 通过 WASM processor 启动所有输出 sinks
                if let Err(e) = processor.start_sinks() {
                    log::error!("Failed to start sinks: {}", e);
                }

                *state = TaskState::Running;
                *shared_state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }

            TaskControlSignal::Stop { completion_flag } => {
                log::info!("Stopping task: {}", task_name);

                // 停止所有输入源
                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.stop() {
                        log::warn!("Failed to stop input {}: {}", idx, e);
                    }
                }

                // 通过 WASM processor 停止所有输出 sinks
                if let Err(e) = processor.stop_sinks() {
                    log::warn!("Failed to stop sinks: {}", e);
                }

                *state = TaskState::Stopped;
                *shared_state.lock().unwrap() = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }

            TaskControlSignal::Cancel { completion_flag } => {
                log::info!("Canceling task: {}", task_name);
                *state = TaskState::Stopped;
                *shared_state.lock().unwrap() = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Exit
            }

            TaskControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                if *state != TaskState::Running {
                    let error = format!("Cannot checkpoint in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }

                log::info!(
                    "Checkpoint {} started for task: {}",
                    checkpoint_id,
                    task_name
                );
                *state = TaskState::Checkpointing;
                *shared_state.lock().unwrap() = ComponentState::Checkpointing;

                // 触发所有输入源的检查点
                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.take_checkpoint(checkpoint_id) {
                        log::error!("Failed to checkpoint input {}: {}", idx, e);
                    }
                }

                // 通过 WASM processor 触发所有输出 sinks 的检查点
                if let Err(e) = processor.take_checkpoint_sinks(checkpoint_id) {
                    log::error!("Failed to checkpoint sinks: {}", e);
                }

                completion_flag.mark_completed();
                ControlAction::Continue
            }

            TaskControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                if *state != TaskState::Checkpointing {
                    let error = format!("Cannot finish checkpoint in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }

                log::info!(
                    "Checkpoint {} finished for task: {}",
                    checkpoint_id,
                    task_name
                );

                // 完成所有输入源的检查点
                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.finish_checkpoint(checkpoint_id) {
                        log::error!("Failed to finish checkpoint for input {}: {}", idx, e);
                    }
                }

                // 通过 WASM processor 完成所有输出 sinks 的检查点
                if let Err(e) = processor.finish_checkpoint_sinks(checkpoint_id) {
                    log::error!("Failed to finish checkpoint for sinks: {}", e);
                }

                *state = TaskState::Running;
                *shared_state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }

            TaskControlSignal::Close { completion_flag } => {
                log::info!("Closing task: {}", task_name);
                *state = TaskState::Closing;
                *shared_state.lock().unwrap() = ComponentState::Closing;

                // 资源会在线程退出时清理
                *state = TaskState::Closed;
                *shared_state.lock().unwrap() = ComponentState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
        }
    }

    // ==================== 数据处理（热点路径，无锁无原子操作）====================

    /// 批量处理数据
    ///
    /// 这是热点路径，没有任何锁或原子变量操作
    #[inline]
    fn process_batch(
        inputs: &mut Vec<Box<dyn InputSource>>,
        processor: &mut Box<dyn WasmProcessor>,
        current_input_index: &mut usize,
    ) {
        let input_count = inputs.len();
        if input_count == 0 {
            return;
        }

        let mut batch_count = 0;

        // 批量处理，减少调度开销
        while batch_count < MAX_BATCH_SIZE {
            // 轮询所有输入源
            let mut found_data = false;

            for _ in 0..input_count {
                let input_idx = *current_input_index;
                let input = &mut inputs[input_idx];
                *current_input_index = (*current_input_index + 1) % input_count;

                match input.get_next() {
                    Ok(Some(data)) => {
                        found_data = true;
                        Self::process_single_record(data, processor, input_idx);
                        batch_count += 1;
                        break; // 处理一条后继续下一轮
                    }
                    Ok(None) => continue, // 没有数据，尝试下一个输入
                    Err(e) => {
                        log::error!("Error reading input: {}", e);
                        continue;
                    }
                }
            }

            if !found_data {
                // 所有输入都没有数据，让出 CPU
                break;
            }
        }
    }

    /// 处理单条记录
    #[inline]
    fn process_single_record(
        data: BufferOrEvent,
        processor: &mut Box<dyn WasmProcessor>,
        input_index: usize,
    ) {
        if !data.is_buffer() {
            return;
        }

        if let Some(buffer_bytes) = data.get_buffer() {
            // 通过处理器处理数据
            if let Err(e) = processor.process(buffer_bytes.to_vec(), input_index) {
                log::error!("Processor error from input {}: {}", input_index, e);
            }
        }
    }

    // ==================== 资源清理 ====================

    fn cleanup_resources(
        inputs: &mut Vec<Box<dyn InputSource>>,
        processor: &mut Box<dyn WasmProcessor>,
        task_name: &str,
    ) {
        // 关闭所有输入源
        for (idx, input) in inputs.iter_mut().enumerate() {
            if let Err(e) = input.stop() {
                log::warn!("Failed to stop input {} for {}: {}", idx, task_name, e);
            }
            if let Err(e) = input.close() {
                log::warn!("Failed to close input {} for {}: {}", idx, task_name, e);
            }
        }

        // 通过 WASM processor 关闭所有输出 sinks
        if let Err(e) = processor.close_sinks() {
            log::warn!("Failed to close sinks for {}: {}", task_name, e);
        }

        // 关闭处理器
        if let Err(e) = processor.close() {
            log::warn!("Failed to close processor for {}: {}", task_name, e);
        }
    }

    // ==================== 公共控制方法（主线程调用）====================

    /// 等待控制操作完成
    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);

        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    if let Some(error) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(
                            format!("{} failed: {}", operation_name, error),
                        )));
                    }
                    return Ok(());
                }
                Err(_) => {
                    log::warn!(
                        "{} timeout (retry {}/{}), task: {}",
                        operation_name,
                        retry + 1,
                        CONTROL_OPERATION_MAX_RETRIES,
                        self.task_name
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

    /// 启动任务
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            sender
                .send(TaskControlSignal::Start {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(
                        format!("Failed to send start signal: {}", e),
                    )) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "Start")
    }

    /// 停止任务
    pub fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            sender
                .send(TaskControlSignal::Stop {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(
                        format!("Failed to send stop signal: {}", e),
                    )) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "Stop")
    }

    /// 取消任务
    pub fn cancel(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            sender
                .send(TaskControlSignal::Cancel {
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(
                        format!("Failed to send cancel signal: {}", e),
                    )) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "Cancel")
    }

    /// 开始检查点
    pub fn take_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            sender
                .send(TaskControlSignal::Checkpoint {
                    checkpoint_id,
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(
                        format!("Failed to send checkpoint signal: {}", e),
                    )) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "Checkpoint")
    }

    /// 完成检查点
    pub fn finish_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            sender
                .send(TaskControlSignal::CheckpointFinish {
                    checkpoint_id,
                    completion_flag: completion_flag.clone(),
                })
                .map_err(|e| {
                    Box::new(std::io::Error::other(
                        format!("Failed to send checkpoint finish signal: {}", e),
                    )) as Box<dyn std::error::Error + Send>
                })?;
        }
        self.wait_with_retry(&completion_flag, "CheckpointFinish")
    }

    /// 关闭任务
    pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            let _ = sender.send(TaskControlSignal::Close {
                completion_flag: completion_flag.clone(),
            });
            let _ = self.wait_with_retry(&completion_flag, "Close");
        }

        // 注意：线程句柄已经移动到线程组中，由 TaskHandle 统一管理
        // 这里不再需要 join，线程组会在 TaskHandle 中统一等待
        // 如果 task_thread 还存在（旧代码路径），则 join 它
        if let Some(handle) = self.task_thread.take()
            && let Err(e) = handle.join() {
                log::warn!("Task thread join error: {:?}", e);
            }

        self.control_sender.take();
        log::info!("WasmTask closed: {}", self.task_name);
        Ok(())
    }

    // ==================== 状态查询 ====================

    /// 获取当前状态
    pub fn get_state(&self) -> ComponentState {
        self.state.lock().unwrap().clone()
    }

    /// 检查是否正在运行
    pub fn is_running(&self) -> bool {
        matches!(self.get_state(), ComponentState::Running)
    }

    /// 获取任务名称
    pub fn get_name(&self) -> &str {
        &self.task_name
    }

    /// 获取线程组信息（用于 TaskThreadPool::submit）
    ///
    /// # 返回值
    /// - `Some(Vec<ThreadGroup>)`: 线程组信息（会从 WasmTask 中移除）
    /// - `None`: 没有线程组信息
    pub fn take_thread_groups(&mut self) -> Option<Vec<ThreadGroup>> {
        self.thread_groups.take()
    }

    // ==================== 线程管理方法 ====================

    /// 等待任务完成
    pub fn wait_for_completion(&self) -> Result<ExecutionState, Box<dyn std::error::Error>> {
        if let Some(rx) = self.termination_future.lock().unwrap().take() {
            rx.recv()
                .map_err(|e| format!("Failed to receive termination state: {}", e).into())
        } else {
            Err("Termination future already consumed".into())
        }
    }

    /// 获取执行状态
    pub fn get_execution_state(&self) -> ExecutionState {
        ExecutionState::from_u8(self.execution_state.load(Ordering::Relaxed))
    }

    /// 获取失败原因
    pub fn get_failure_cause(&self) -> Option<String> {
        self.failure_cause.lock().unwrap().clone()
    }

    /// 检查线程是否还在运行
    pub fn is_thread_alive(&self) -> bool {
        self.thread_running.load(Ordering::Relaxed)
    }

    /// 等待线程完成（用于测试和调试）
    pub fn join_thread(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(handle) = self.task_thread.take() {
            handle
                .join()
                .map_err(|e| format!("Thread join error: {:?}", e))?;
        }
        Ok(())
    }

    /// 尝试等待线程完成（非阻塞检查）
    pub fn try_join_thread(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        // 检查线程是否还在运行
        if !self.thread_running.load(Ordering::Relaxed) {
            // 线程已结束，尝试 join
            if let Some(handle) = self.task_thread.take() {
                handle
                    .join()
                    .map_err(|e| format!("Thread join error: {:?}", e))?;
                return Ok(true);
            }
            return Ok(true); // 没有线程句柄，认为已完成
        }
        Ok(false) // 线程还在运行
    }

    /// 强制等待线程完成（带超时）
    pub fn wait_thread_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        // 等待线程结束标志
        while self.thread_running.load(Ordering::Relaxed) {
            if start.elapsed() > timeout {
                return Ok(false); // 超时
            }
            thread::sleep(Duration::from_millis(10));
        }

        // 线程已结束，join 它
        if let Some(handle) = self.task_thread.take() {
            handle
                .join()
                .map_err(|e| format!("Thread join error: {:?}", e))?;
        }

        Ok(true)
    }
}

impl TaskLifecycle for WasmTask {
    fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 直接调用内部的 init_with_context 方法（线程池已包含在 InitContext 中）
        <WasmTask>::init_with_context(self, init_context)
    }

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 使用完全限定语法调用原有的 start 方法，避免递归
        <WasmTask>::start(self)
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 使用完全限定语法调用原有的 stop 方法
        <WasmTask>::stop(self)
    }

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 使用完全限定语法调用原有的 take_checkpoint 方法
        <WasmTask>::take_checkpoint(self, checkpoint_id)
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        // 使用完全限定语法调用原有的 close 方法
        <WasmTask>::close(self)
    }

    fn get_state(&self) -> ComponentState {
        // 使用完全限定语法调用原有的 get_state 方法
        <WasmTask>::get_state(self)
    }

    fn get_name(&self) -> &str {
        &self.task_name
    }
}

impl Drop for WasmTask {
    fn drop(&mut self) {
        if self.task_thread.is_some() {
            let _ = self.close();
        }
    }
}
