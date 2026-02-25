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

use super::thread_pool::ThreadGroup;
use super::wasm_processor_trait::WasmProcessor;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::input::Input;
use crate::runtime::output::Output;
use crate::runtime::processor::function_error::FunctionErrorReport;
use crate::runtime::task::{ControlMailBox, TaskControlSignal, TaskLifecycle};
use crate::storage::task::FunctionInfo;
use crossbeam_channel::{Receiver, unbounded};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;
const CONTROL_OPERATION_MAX_RETRIES: u32 = 3;
const MAX_BATCH_SIZE: usize = 1000;

enum ControlAction {
    Continue,
    Pause,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    Created,
    Deploying,
    Initializing,
    Running,
    Finished,
    Canceling,
    Canceled,
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
    task_name: String,
    task_type: String,
    inputs: Option<Vec<Box<dyn Input>>>,
    processor: Option<Box<dyn WasmProcessor>>,
    outputs: Option<Vec<Box<dyn Output>>>,
    state: Arc<Mutex<ComponentState>>,
    control_mailbox: Option<Arc<ControlMailBox>>,
    control_receiver: Option<Receiver<TaskControlSignal>>,
    task_thread: Option<JoinHandle<()>>,
    thread_groups: Option<Vec<ThreadGroup>>,
    execution_state: Arc<Mutex<ExecutionState>>,
    failure_cause: Arc<Mutex<Option<String>>>,
    thread_running: Arc<AtomicBool>,
    termination_future: Arc<Mutex<Option<mpsc::Receiver<ExecutionState>>>>,
    create_time: u64,
}

impl WasmTask {
    pub fn new(
        task_name: String,
        task_type: String,
        inputs: Vec<Box<dyn Input>>,
        processor: Box<dyn WasmProcessor>,
        outputs: Vec<Box<dyn Output>>,
        create_time: u64,
    ) -> Self {
        let (_tx, rx) = mpsc::channel();
        let (control_sender, control_receiver) = unbounded();
        let mailbox = Arc::new(ControlMailBox::new(control_sender));
        Self {
            task_name,
            task_type,
            inputs: Some(inputs),
            processor: Some(processor),
            outputs: Some(outputs),
            state: Arc::new(Mutex::new(ComponentState::Uninitialized)),
            control_mailbox: Some(mailbox),
            control_receiver: Some(control_receiver),
            task_thread: None,
            thread_groups: None,
            execution_state: Arc::new(Mutex::new(ExecutionState::Created)),
            failure_cause: Arc::new(Mutex::new(None)),
            thread_running: Arc::new(AtomicBool::new(false)),
            termination_future: Arc::new(Mutex::new(Some(rx))),
            create_time,
        }
    }

    pub fn mailbox(&self) -> Option<&ControlMailBox> {
        self.control_mailbox.as_deref()
    }

    pub fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut inputs = self.inputs.take().ok_or_else(|| {
            Box::new(std::io::Error::other("inputs already moved to thread"))
                as Box<dyn std::error::Error + Send>
        })?;
        let mut processor = self.processor.take().ok_or_else(|| {
            Box::new(std::io::Error::other("processor already moved to thread"))
                as Box<dyn std::error::Error + Send>
        })?;
        let mut outputs = self.outputs.take().ok_or_else(|| {
            Box::new(std::io::Error::other("outputs already moved to thread"))
                as Box<dyn std::error::Error + Send>
        })?;

        let init_context = init_context.clone();

        for (idx, out) in outputs.iter_mut().enumerate() {
            if let Err(e) = out.init_with_context(&init_context) {
                log::error!("Failed to init output {}: {}", idx, e);
                return Err(Box::new(std::io::Error::other(format!(
                    "Failed to init output {}: {}",
                    idx, e
                ))));
            }
        }

        if let Err(e) = processor.init_with_context(&init_context) {
            log::error!("Failed to init processor: {}", e);
            return Err(Box::new(std::io::Error::other(format!(
                "Failed to init processor: {}",
                e
            ))));
        }

        let create_time = self.get_create_time();
        if let Err(e) =
            processor.init_wasm_host(outputs, &init_context, self.task_name.clone(), create_time)
        {
            log::error!("Failed to init WasmHost: {}", e);
            return Err(Box::new(std::io::Error::other(format!(
                "Failed to init WasmHost: {}",
                e
            ))));
        }

        for (idx, input) in inputs.iter_mut().enumerate() {
            if let Err(e) = input.init_with_context(&init_context) {
                log::error!("Failed to init input {}: {}", idx, e);
                return Err(Box::new(std::io::Error::other(format!(
                    "Failed to init input {}: {}",
                    idx, e
                ))));
            }
        }

        let control_receiver = self.control_receiver.take().ok_or_else(|| {
            Box::new(std::io::Error::other(
                "control_receiver already consumed by deploy",
            )) as Box<dyn std::error::Error + Send>
        })?;

        let task_name = self.task_name.clone();
        let state = self.state.clone();
        let execution_state = self.execution_state.clone();
        let thread_running = self.thread_running.clone();
        let termination_tx = {
            let (_tx, rx) = mpsc::channel();
            *self.termination_future.lock().unwrap() = Some(rx);
            _tx
        };

        thread_running.store(true, Ordering::Relaxed);
        *self.execution_state.lock().unwrap() = ExecutionState::Initializing;

        let failure_cause = self.failure_cause.clone();
        let init_context_for_loop = init_context.clone();

        let thread_handle = thread::Builder::new()
            .name(format!("stream-task-{}", task_name))
            .spawn(move || {
                Self::task_thread_loop(
                    task_name,
                    inputs,
                    processor,
                    control_receiver,
                    state,
                    failure_cause,
                    execution_state.clone(),
                    init_context_for_loop,
                );

                let state = *execution_state.lock().unwrap();
                let report = if state == ExecutionState::Failed {
                    ExecutionState::Failed
                } else {
                    ExecutionState::Finished
                };
                if state != ExecutionState::Failed {
                    *execution_state.lock().unwrap() = ExecutionState::Finished;
                }
                thread_running.store(false, Ordering::Relaxed);
                let _ = termination_tx.send(report);
            })
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to start task thread: {}",
                    e
                ))) as Box<dyn std::error::Error + Send>
            })?;

        use crate::runtime::processor::wasm::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut main_runloop_group = ThreadGroup::new(
            ThreadGroupType::MainRunloop,
            format!("MainRunloop-{}", self.task_name),
        );
        main_runloop_group.add_thread(thread_handle);
        init_context.register_thread_group(main_runloop_group);

        let thread_groups = init_context.take_thread_groups();
        self.thread_groups = Some(thread_groups);

        self.task_thread = None;

        Ok(())
    }

    fn task_thread_loop(
        task_name: String,
        mut inputs: Vec<Box<dyn Input>>,
        mut processor: Box<dyn WasmProcessor>,
        control_receiver: Receiver<TaskControlSignal>,
        shared_state: Arc<Mutex<ComponentState>>,
        failure_cause: Arc<Mutex<Option<String>>>,
        execution_state: Arc<Mutex<ExecutionState>>,
        _init_context: crate::runtime::taskexecutor::InitContext,
    ) {
        let thread_start_time = std::time::Instant::now();
        use crossbeam_channel::select;

        let init_start = std::time::Instant::now();
        let mut state = TaskState::Initialized;
        let mut current_input_index: usize = 0;
        let mut is_running = false;
        let init_elapsed = init_start.elapsed().as_secs_f64();
        log::debug!(
            "[Timing] task_thread_loop - Initialize local state: {:.3}s",
            init_elapsed
        );

        let lock_start = std::time::Instant::now();
        *shared_state.lock().unwrap() = ComponentState::Initialized;
        let lock_elapsed = lock_start.elapsed().as_secs_f64();
        log::debug!(
            "[Timing] task_thread_loop - Update shared state: {:.3}s",
            lock_elapsed
        );

        let thread_init_elapsed = thread_start_time.elapsed().as_secs_f64();
        log::debug!(
            "Task thread started (paused): {} (thread init: {:.3}s)",
            task_name,
            thread_init_elapsed
        );

        loop {
            if is_running {
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
                                    &failure_cause,
                                    &execution_state,
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
                        if let Err(report) = Self::process_batch(
                            &mut inputs,
                            &mut processor,
                            &mut current_input_index,
                        ) {
                            let msg = report.to_string();
                            log::error!("Task {} received error signal: {}", task_name, msg);
                            *failure_cause.lock().unwrap() = Some(msg.clone());
                            *shared_state.lock().unwrap() = ComponentState::Error { error: msg };
                            *execution_state.lock().unwrap() = ExecutionState::Failed;
                            break;
                        }
                    }
                }
            } else {
                match control_receiver.recv() {
                    Ok(signal) => {
                        match Self::handle_control_signal(
                            signal,
                            &mut state,
                            &mut inputs,
                            &mut processor,
                            &shared_state,
                            &task_name,
                            &failure_cause,
                            &execution_state,
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

        Self::cleanup_resources(&mut inputs, &mut processor, &task_name);
        log::info!("Task thread exiting: {}", task_name);
    }

    fn handle_control_signal(
        signal: TaskControlSignal,
        state: &mut TaskState,
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        shared_state: &Arc<Mutex<ComponentState>>,
        task_name: &str,
        failure_cause: &Arc<Mutex<Option<String>>>,
        execution_state: &Arc<Mutex<ExecutionState>>,
    ) -> ControlAction {
        match signal {
            TaskControlSignal::Start { completion_flag } => {
                if !matches!(
                    *state,
                    TaskState::Initialized | TaskState::Stopped | TaskState::Failed
                ) {
                    let error = format!("Cannot start in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }

                log::debug!("Starting task: {}", task_name);

                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.start() {
                        log::error!("Failed to start input {}: {}", idx, e);
                    }
                }

                if let Err(e) = processor.start_outputs() {
                    log::error!("Failed to start outputs: {}", e);
                }

                *state = TaskState::Running;
                *shared_state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }

            TaskControlSignal::Stop { completion_flag } => {
                if !matches!(*state, TaskState::Running | TaskState::Checkpointing) {
                    let error = format!("Cannot stop in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }

                log::info!("Stopping task: {}", task_name);

                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.stop() {
                        log::warn!("Failed to stop input {}: {}", idx, e);
                    }
                }

                if let Err(e) = processor.stop_outputs() {
                    log::warn!("Failed to stop outputs: {}", e);
                }

                *state = TaskState::Stopped;
                *shared_state.lock().unwrap() = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }

            TaskControlSignal::Cancel { completion_flag } => {
                if !matches!(
                    *state,
                    TaskState::Initialized
                        | TaskState::Running
                        | TaskState::Stopped
                        | TaskState::Checkpointing
                        | TaskState::Failed
                ) {
                    let error = format!("Cannot cancel in state: {:?}", state);
                    log::error!("{} for task: {}", error, task_name);
                    completion_flag.mark_error(error);
                    return ControlAction::Continue;
                }

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

                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.take_checkpoint(checkpoint_id) {
                        log::error!("Failed to checkpoint input {}: {}", idx, e);
                    }
                }

                if let Err(e) = processor.take_checkpoint_outputs(checkpoint_id) {
                    log::error!("Failed to checkpoint outputs: {}", e);
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

                for (idx, input) in inputs.iter_mut().enumerate() {
                    if let Err(e) = input.finish_checkpoint(checkpoint_id) {
                        log::error!("Failed to finish checkpoint for input {}: {}", idx, e);
                    }
                }

                if let Err(e) = processor.finish_checkpoint_outputs(checkpoint_id) {
                    log::error!("Failed to finish checkpoint for outputs: {}", e);
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

                *state = TaskState::Closed;
                *shared_state.lock().unwrap() = ComponentState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }

            TaskControlSignal::ErrorReport(report) => {
                let msg = report.to_string();
                log::error!(
                    "Task {} received error report via MailBox: {}",
                    task_name,
                    msg
                );
                for (idx, input) in inputs.iter().enumerate() {
                    if let Err(e) = input.set_error_state() {
                        log::error!("Failed to set error state on input {}: {}", idx, e);
                    }
                }
                *state = TaskState::Failed;
                *failure_cause.lock().unwrap() = Some(msg.clone());
                *shared_state.lock().unwrap() = ComponentState::Error { error: msg.clone() };
                *execution_state.lock().unwrap() = ExecutionState::Failed;
                if let Err(e) = processor.set_error_state_outputs() {
                    log::error!("Failed to set error state on outputs: {}", e);
                }
                ControlAction::Pause
            }
        }
    }

    #[inline]
    fn process_batch(
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        current_input_index: &mut usize,
    ) -> Result<(), FunctionErrorReport> {
        let input_count = inputs.len();
        if input_count == 0 {
            return Ok(());
        }

        let mut batch_count = 0;

        while batch_count < MAX_BATCH_SIZE {
            let mut found_data = false;

            for _ in 0..input_count {
                let input_idx = *current_input_index;
                let input = &mut inputs[input_idx];
                *current_input_index = (*current_input_index + 1) % input_count;

                match input.get_next() {
                    Ok(Some(data)) => {
                        found_data = true;
                        Self::process_single_record(data, processor, input.get_group_id())?;
                        batch_count += 1;
                        break;
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        return Err(FunctionErrorReport::input(input_idx, e.to_string()));
                    }
                }
            }

            if !found_data {
                break;
            }
        }

        Ok(())
    }

    #[inline]
    fn process_single_record(
        data: BufferOrEvent,
        processor: &mut Box<dyn WasmProcessor>,
        input_index: usize,
    ) -> Result<(), FunctionErrorReport> {
        if !data.is_buffer() {
            return Ok(());
        }

        if let Some(buffer_bytes) = data.get_buffer() {
            processor
                .process(buffer_bytes.to_vec(), input_index)
                .map_err(|e| FunctionErrorReport::processor(input_index, e.to_string()))?;
        }
        Ok(())
    }

    fn cleanup_resources(
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        task_name: &str,
    ) {
        for (idx, input) in inputs.iter_mut().enumerate() {
            if let Err(e) = input.stop() {
                log::warn!("Failed to stop input {} for {}: {}", idx, task_name, e);
            }
            if let Err(e) = input.close() {
                log::warn!("Failed to close input {} for {}: {}", idx, task_name, e);
            }
        }

        if let Err(e) = processor.close_outputs() {
            log::warn!("Failed to close outputs for {}: {}", task_name, e);
        }

        if let Err(e) = processor.close() {
            log::warn!("Failed to close processor for {}: {}", task_name, e);
        }
    }

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
                        return Err(Box::new(std::io::Error::other(format!(
                            "{} failed: {}",
                            operation_name, error
                        ))));
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

    pub fn start(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            mailbox.send_start(completion_flag.clone())?;
        }
        self.wait_with_retry(&completion_flag, "Start")
    }

    pub fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            mailbox.send_stop(completion_flag.clone())?;
        }
        self.wait_with_retry(&completion_flag, "Stop")
    }

    pub fn cancel(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            mailbox.send_cancel(completion_flag.clone())?;
        }
        self.wait_with_retry(&completion_flag, "Cancel")
    }

    pub fn take_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            mailbox.send_checkpoint(checkpoint_id, completion_flag.clone())?;
        }
        self.wait_with_retry(&completion_flag, "Checkpoint")
    }

    pub fn finish_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            mailbox.send_finish_checkpoint(checkpoint_id, completion_flag.clone())?;
        }
        self.wait_with_retry(&completion_flag, "CheckpointFinish")
    }

    pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let completion_flag = TaskCompletionFlag::new();
        if let Some(ref mailbox) = self.control_mailbox {
            let _ = mailbox.send_close(completion_flag.clone());
            let _ = self.wait_with_retry(&completion_flag, "Close");
        }

        if let Some(handle) = self.task_thread.take()
            && let Err(e) = handle.join()
        {
            log::warn!("Task thread join error: {:?}", e);
        }

        self.control_mailbox.take();
        log::info!("WasmTask closed: {}", self.task_name);
        Ok(())
    }

    pub fn get_state(&self) -> ComponentState {
        self.state.lock().unwrap().clone()
    }

    pub fn is_running(&self) -> bool {
        matches!(self.get_state(), ComponentState::Running)
    }

    pub fn get_name(&self) -> &str {
        &self.task_name
    }

    pub fn get_create_time(&self) -> u64 {
        self.create_time
    }

    pub fn take_thread_groups(&mut self) -> Option<Vec<ThreadGroup>> {
        self.thread_groups.take()
    }

    pub fn wait_for_completion(&self) -> Result<ExecutionState, Box<dyn std::error::Error>> {
        if let Some(rx) = self.termination_future.lock().unwrap().take() {
            rx.recv()
                .map_err(|e| format!("Failed to receive termination state: {}", e).into())
        } else {
            Err("Termination future already consumed".into())
        }
    }

    pub fn get_execution_state(&self) -> ExecutionState {
        *self.execution_state.lock().unwrap()
    }

    pub fn get_failure_cause(&self) -> Option<String> {
        self.failure_cause.lock().unwrap().clone()
    }

    pub fn is_thread_alive(&self) -> bool {
        self.thread_running.load(Ordering::Relaxed)
    }

    pub fn join_thread(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(handle) = self.task_thread.take() {
            handle
                .join()
                .map_err(|e| format!("Thread join error: {:?}", e))?;
        }
        Ok(())
    }

    pub fn try_join_thread(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        if !self.thread_running.load(Ordering::Relaxed) {
            if let Some(handle) = self.task_thread.take() {
                handle
                    .join()
                    .map_err(|e| format!("Thread join error: {:?}", e))?;
                return Ok(true);
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub fn wait_thread_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        while self.thread_running.load(Ordering::Relaxed) {
            if start.elapsed() > timeout {
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(10));
        }

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
        <WasmTask>::init_with_context(self, init_context)
    }

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        <WasmTask>::start(self)
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        <WasmTask>::stop(self)
    }

    fn take_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        <WasmTask>::take_checkpoint(self, checkpoint_id)
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        <WasmTask>::close(self)
    }

    fn get_state(&self) -> ComponentState {
        <WasmTask>::get_state(self)
    }

    fn get_name(&self) -> &str {
        &self.task_name
    }

    fn get_control_mailbox(&self) -> Option<Arc<ControlMailBox>> {
        self.control_mailbox.clone()
    }

    fn get_function_info(&self) -> FunctionInfo {
        FunctionInfo {
            name: self.task_name.clone(),
            task_type: self.task_type.clone(),
            status: format!("{:?}", self.get_state()),
            create_time: self.get_create_time(),
        }
    }
}

impl Drop for WasmTask {
    fn drop(&mut self) {
        if self.task_thread.is_some() {
            let _ = self.close();
        }
    }
}
