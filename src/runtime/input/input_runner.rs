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

use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::common::TaskCompletionFlag;
use crate::runtime::input::input_protocol::InputProtocol;
use crate::runtime::input::{Input, InputState};
use crate::runtime::processor::function_error::FunctionErrorReport;
use crate::runtime::task::ControlMailBox;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_CHANNEL_CAPACITY: usize = 10000;
const MAX_BATCH_CONSUME_SIZE: usize = 10000;
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;
const CONTROL_OPERATION_MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone)]
enum InputControlSignal {
    Start {
        completion_flag: TaskCompletionFlag,
    },
    Stop {
        completion_flag: TaskCompletionFlag,
    },
    Close {
        completion_flag: TaskCompletionFlag,
    },
    Checkpoint {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    CheckpointFinish {
        checkpoint_id: u64,
        completion_flag: TaskCompletionFlag,
    },
    Error {
        completion_flag: TaskCompletionFlag,
        error: String,
    },
}

enum ControlAction {
    Continue,
    Pause,
    Exit,
    Unchanged,
}

pub struct InputRunner<P: InputProtocol> {
    protocol: Arc<P>,
    group_id: usize,
    input_id: usize,
    state: Arc<Mutex<InputState>>,
    mail_box: Option<Arc<ControlMailBox>>,
    data_sender: Option<Sender<BufferOrEvent>>,
    data_receiver: Option<Receiver<BufferOrEvent>>,
    control_sender: Option<Sender<InputControlSignal>>,
    control_receiver: Option<Receiver<InputControlSignal>>,
}

impl<P: InputProtocol> InputRunner<P> {
    pub fn new(protocol: P, group_id: usize, input_id: usize) -> Self {
        Self {
            protocol: Arc::new(protocol),
            group_id,
            input_id,
            state: Arc::new(Mutex::new(InputState::Uninitialized)),
            mail_box: None,
            data_sender: None,
            data_receiver: None,
            control_sender: None,
            control_receiver: None,
        }
    }

    fn wait_with_retry(
        &self,
        completion_flag: &TaskCompletionFlag,
        op: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);
        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match completion_flag.wait_timeout(timeout) {
                Ok(_) => {
                    if let Some(err) = completion_flag.get_error() {
                        return Err(Box::new(std::io::Error::other(format!(
                            "{} failed: {}",
                            op, err
                        ))));
                    }
                    return Ok(());
                }
                Err(_) => log::warn!(
                    "{} timeout (retry {}/{})",
                    op,
                    retry + 1,
                    CONTROL_OPERATION_MAX_RETRIES
                ),
            }
        }
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{} timed out", op),
        )))
    }

    fn worker_loop(
        protocol: Arc<P>,
        data_sender: Sender<BufferOrEvent>,
        control_receiver: Receiver<InputControlSignal>,
        control_sender: Sender<InputControlSignal>,
        state: Arc<Mutex<InputState>>,
        mail_box: Option<Arc<ControlMailBox>>,
        input_id: usize,
    ) {
        use crossbeam_channel::select;
        let mut is_running = false;

        loop {
            if is_running {
                select! {
                    recv(control_receiver) -> res => match res {
                        Ok(sig) => match Self::handle_signal(sig, &protocol, &state) {
                            ControlAction::Continue => is_running = true,
                            ControlAction::Pause => is_running = false,
                            ControlAction::Exit => break,
                            ControlAction::Unchanged => {},
                        },
                        Err(_) => break,
                    },
                    default() => {
                        if data_sender.is_full() {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        for _ in 0..MAX_BATCH_CONSUME_SIZE {
                            match protocol.poll(Duration::from_millis(1000)) {
                                Ok(Some(data)) => {
                                    if data_sender.send(data).is_err() { break; }
                                    if data_sender.is_full() { break; }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    let msg = e.to_string();
                                    let _ = control_sender.send(InputControlSignal::Error {
                                        completion_flag: TaskCompletionFlag::new(),
                                        error: msg.clone(),
                                    });
                                    if let Some(m) = &mail_box {
                                        let _ = m.send_error_report(FunctionErrorReport::input(input_id, msg));
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                match control_receiver.recv() {
                    Ok(sig) => match Self::handle_signal(sig, &protocol, &state) {
                        ControlAction::Continue => is_running = true,
                        ControlAction::Pause => is_running = false,
                        ControlAction::Exit => break,
                        ControlAction::Unchanged => {}
                    },
                    Err(_) => break,
                }
            }
        }
    }

    fn handle_signal(
        sig: InputControlSignal,
        protocol: &Arc<P>,
        state: &Arc<Mutex<InputState>>,
    ) -> ControlAction {
        let mut s = state.lock().unwrap();
        match sig {
            InputControlSignal::Start { completion_flag } => {
                if let Err(e) = protocol.on_start() {
                    *s = InputState::Error {
                        error: e.to_string(),
                    };
                    completion_flag.mark_error(e.to_string());
                    return ControlAction::Unchanged;
                }
                *s = InputState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            InputControlSignal::Stop { completion_flag } => {
                let _ = protocol.on_stop();
                *s = InputState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }
            InputControlSignal::Close { completion_flag } => {
                let _ = protocol.on_close();
                *s = InputState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            InputControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                let _ = protocol.on_checkpoint(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            InputControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                let _ = protocol.on_checkpoint_finish(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            InputControlSignal::Error {
                completion_flag,
                error,
            } => {
                *s = InputState::Error { error };
                completion_flag.mark_completed();
                ControlAction::Pause
            }
        }
    }
}

impl<P: InputProtocol> Input for InputRunner<P> {
    fn init_with_context(
        &mut self,
        init_context: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        if !matches!(*self.state.lock().unwrap(), InputState::Uninitialized) {
            return Ok(());
        }

        let mailbox_handle = init_context.get_control_mailbox();
        if let Some(ref m) = *mailbox_handle.lock().unwrap() {
            self.mail_box = Some(Arc::clone(m));
        }

        self.protocol.init()?;

        let (data_sender, data_receiver) = bounded(DEFAULT_CHANNEL_CAPACITY);
        let (control_sender, control_receiver) = unbounded();

        self.data_sender = Some(data_sender.clone());
        self.data_receiver = Some(data_receiver);
        self.control_sender = Some(control_sender.clone());

        let protocol = self.protocol.clone();
        let state = self.state.clone();
        let mail_box = self.mail_box.clone();
        let input_id = self.input_id;

        let thread_handle = thread::Builder::new()
            .name(format!("input-{}-{}", protocol.name(), input_id))
            .spawn(move || {
                Self::worker_loop(
                    protocol,
                    data_sender,
                    control_receiver,
                    control_sender,
                    state,
                    mail_box,
                    input_id,
                );
            })
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;

        use crate::runtime::processor::wasm::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut group = ThreadGroup::new(
            ThreadGroupType::Input(self.group_id),
            format!("Input-g{}", self.group_id),
        );
        group.add_thread(thread_handle);
        init_context.register_thread_group(group);

        *self.state.lock().unwrap() = InputState::Initialized;
        Ok(())
    }

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(InputControlSignal::Start {
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Start")
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(InputControlSignal::Stop {
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Stop")
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            let _ = sender.send(InputControlSignal::Close {
                completion_flag: flag.clone(),
            });
            let _ = self.wait_with_retry(&flag, "Close");
        }
        self.data_sender.take();
        self.data_receiver.take();
        self.control_sender.take();
        Ok(())
    }

    fn get_next(&mut self) -> Result<Option<BufferOrEvent>, Box<dyn std::error::Error + Send>> {
        match self.data_receiver.as_ref().map(|r| r.try_recv()) {
            Some(Ok(data)) => Ok(Some(data)),
            _ => Ok(None),
        }
    }

    fn take_checkpoint(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(InputControlSignal::Checkpoint {
                checkpoint_id: id,
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Checkpoint")
    }

    fn finish_checkpoint(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(InputControlSignal::CheckpointFinish {
                checkpoint_id: id,
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "CheckpointFinish")
    }

    fn get_group_id(&self) -> usize {
        self.group_id
    }

    fn set_error_state(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(InputControlSignal::Error {
                completion_flag: flag.clone(),
                error: "manual error".into(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "SetErrorState")
    }
}
