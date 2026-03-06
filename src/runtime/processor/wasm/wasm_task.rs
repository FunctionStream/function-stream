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

use super::input_strategy::{InputStrategy, RoundRobinStrategy, from_selector_name};
use super::thread_pool::ThreadGroup;
use super::wasm_processor_trait::WasmProcessor;
use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::input::Input;
use crate::runtime::output::Output;
use crate::runtime::processor::function_error::FunctionErrorReport;
use crate::runtime::task::ProcessorRuntimeConfig;
use crate::runtime::task::{ControlMailBox, TaskControlSignal, TaskLifecycle};
use crate::storage::task::FunctionInfo;
use crossbeam_channel::{Receiver, after, select, unbounded};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum ControlAction {
    Continue,
    Pause,
    Exit,
}

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

pub struct WasmTask {
    task_name: String,
    task_type: String,
    input_selector: String,
    processor_runtime: ProcessorRuntimeConfig,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_name: String,
        task_type: String,
        input_selector: String,
        processor_runtime: ProcessorRuntimeConfig,
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
            input_selector,
            processor_runtime,
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
        let input_selector = self.input_selector.clone();
        let processor_runtime = self.processor_runtime.clone();

        let thread_handle = thread::Builder::new()
            .name(format!("stream-task-{}", task_name))
            .spawn(move || {
                Self::task_thread_loop(
                    task_name,
                    input_selector,
                    processor_runtime,
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

    #[allow(clippy::too_many_arguments)]
    fn task_thread_loop(
        task_name: String,
        input_selector: String,
        processor_runtime: ProcessorRuntimeConfig,
        mut inputs: Vec<Box<dyn Input>>,
        mut processor: Box<dyn WasmProcessor>,
        control_receiver: Receiver<TaskControlSignal>,
        shared_state: Arc<Mutex<ComponentState>>,
        failure_cause: Arc<Mutex<Option<String>>>,
        execution_state: Arc<Mutex<ExecutionState>>,
        _init_context: crate::runtime::taskexecutor::InitContext,
    ) {
        let mut state = TaskState::Initialized;
        let mut last_idx: usize = 0;
        let mut finished_mask: u64 = 0;
        let mut is_running = false;
        let mut idle_count: u64 = 0;

        let mut strategy: Box<dyn InputStrategy> =
            from_selector_name(&input_selector).unwrap_or_else(|| Box::new(RoundRobinStrategy));

        *shared_state.lock().unwrap() = ComponentState::Initialized;

        loop {
            if is_running {
                select! {
                    recv(control_receiver) -> result => {
                        idle_count = 0;
                        if let Ok(signal) = result {
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
                        } else {
                            break;
                        }
                    }
                    default => {
                        match Self::process_batch(
                            &mut inputs,
                            &mut processor,
                            &mut last_idx,
                            &mut finished_mask,
                            strategy.as_mut(),
                            &processor_runtime,
                        ) {
                            Ok(true) => {
                                idle_count = 0;
                            }
                            Ok(false) => {
                                idle_count += 1;
                                if idle_count >= processor_runtime.max_idle_count {
                                    select! {
                                        recv(control_receiver) -> res => {
                                            idle_count = 0;
                                            if let Ok(sig) = res {
                                                match Self::handle_control_signal(
                                                    sig,
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
                                            } else {
                                                break;
                                            }
                                        }
                                        recv(after(Duration::from_millis(processor_runtime.idle_sleep_ms))) -> _ => {
                                            idle_count = 0;
                                        }
                                    }
                                }
                            }
                            Err(report) => {
                                let msg = report.to_string();
                                *failure_cause.lock().unwrap() = Some(msg.clone());
                                *shared_state.lock().unwrap() =
                                    ComponentState::Error { error: msg };
                                *execution_state.lock().unwrap() = ExecutionState::Failed;
                                break;
                            }
                        }
                    }
                }
            } else if let Ok(signal) = control_receiver.recv() {
                if matches!(
                    Self::handle_control_signal(
                        signal,
                        &mut state,
                        &mut inputs,
                        &mut processor,
                        &shared_state,
                        &task_name,
                        &failure_cause,
                        &execution_state,
                    ),
                    ControlAction::Exit
                ) {
                    break;
                }
                if state == TaskState::Running {
                    is_running = true;
                }
            } else {
                break;
            }
        }
        Self::cleanup_resources(&mut inputs, &mut processor, &task_name);
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_control_signal(
        signal: TaskControlSignal,
        state: &mut TaskState,
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        shared_state: &Arc<Mutex<ComponentState>>,
        _task_name: &str,
        failure_cause: &Arc<Mutex<Option<String>>>,
        execution_state: &Arc<Mutex<ExecutionState>>,
    ) -> ControlAction {
        match signal {
            TaskControlSignal::Start { completion_flag } => {
                for input in inputs.iter_mut() {
                    let _ = input.start();
                }
                let _ = processor.start_outputs();
                *state = TaskState::Running;
                *shared_state.lock().unwrap() = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            TaskControlSignal::Stop { completion_flag } => {
                for input in inputs.iter_mut() {
                    let _ = input.stop();
                }
                let _ = processor.stop_outputs();
                *state = TaskState::Stopped;
                *shared_state.lock().unwrap() = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }
            TaskControlSignal::Cancel { completion_flag } => {
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            TaskControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                for input in inputs.iter_mut() {
                    let _ = input.take_checkpoint(checkpoint_id);
                }
                let _ = processor.take_checkpoint_outputs(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            TaskControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                for input in inputs.iter_mut() {
                    let _ = input.finish_checkpoint(checkpoint_id);
                }
                let _ = processor.finish_checkpoint_outputs(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            TaskControlSignal::Close { completion_flag } => {
                *state = TaskState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            TaskControlSignal::ErrorReport(report) => {
                let msg = report.to_string();
                *failure_cause.lock().unwrap() = Some(msg.clone());
                *shared_state.lock().unwrap() = ComponentState::Error { error: msg };
                *execution_state.lock().unwrap() = ExecutionState::Failed;
                ControlAction::Pause
            }
        }
    }

    #[inline]
    fn process_batch(
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        last_idx: &mut usize,
        finished_mask: &mut u64,
        strategy: &mut dyn InputStrategy,
        processor_runtime: &ProcessorRuntimeConfig,
    ) -> Result<bool, FunctionErrorReport> {
        let input_count = inputs.len();
        if input_count == 0 || *finished_mask == (1u64 << input_count) - 1 {
            return Ok(false);
        }

        let mut batch_count = 0;
        let mut made_progress = false;
        let max_batch = processor_runtime.max_batch_size;

        while batch_count < max_batch {
            let mask = strategy.next_mask(input_count, *last_idx, *finished_mask);
            if mask == 0 {
                break;
            }

            let i = mask.trailing_zeros() as usize;

            match inputs[i].get_next() {
                Ok(Some(data)) => {
                    Self::process_single_record(data, processor, inputs[i].get_group_id())?;
                    *last_idx = i;
                    made_progress = true;
                    batch_count += 1;
                }
                Ok(None) => break,
                Err(_) => {
                    *finished_mask |= 1 << i;
                    made_progress = true;
                }
            }
        }
        Ok(made_progress)
    }

    #[inline]
    fn process_single_record(
        data: BufferOrEvent,
        processor: &mut Box<dyn WasmProcessor>,
        input_index: usize,
    ) -> Result<(), FunctionErrorReport> {
        if let Some(buf) = data.get_buffer() {
            processor
                .process(buf.to_vec(), input_index)
                .map_err(|e| FunctionErrorReport::processor(input_index, e.to_string()))?;
        }
        Ok(())
    }

    fn cleanup_resources(
        inputs: &mut [Box<dyn Input>],
        processor: &mut Box<dyn WasmProcessor>,
        _task_name: &str,
    ) {
        for input in inputs {
            let _ = input.stop();
            let _ = input.close();
        }
        let _ = processor.close_outputs();
        let _ = processor.close();
    }

    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        operation_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = Duration::from_millis(self.processor_runtime.control_timeout_ms);
        let max_retries = self.processor_runtime.control_max_retries;

        for retry in 0..max_retries {
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
                        max_retries,
                        self.task_name
                    );
                }
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{} failed after {} retries", operation_name, max_retries),
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
