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
use crate::runtime::common::{ComponentState, TaskCompletionFlag};
use crate::runtime::output::Output;
use crate::runtime::output::output_protocol::OutputProtocol;
use crate::runtime::processor::function_error::FunctionErrorReport;
use crate::runtime::task::ControlMailBox;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_CHANNEL_CAPACITY: usize = 10000;
const MAX_BATCH_CONSUME_SIZE: usize = 10000;
const CONTROL_OPERATION_TIMEOUT_MS: u64 = 5000;
const CONTROL_OPERATION_MAX_RETRIES: u32 = 5;

#[derive(Debug, Clone)]
enum OutputControlSignal {
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
    Flush {
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

pub struct OutputRunner<P: OutputProtocol> {
    protocol: Arc<P>,
    output_id: usize,
    state: Arc<Mutex<ComponentState>>,
    mail_box: Option<Arc<ControlMailBox>>,
    data_sender: Option<Sender<BufferOrEvent>>,
    control_sender: Option<Sender<OutputControlSignal>>,
}

impl<P: OutputProtocol> OutputRunner<P> {
    pub fn new(protocol: P, output_id: usize) -> Self {
        Self {
            protocol: Arc::new(protocol),
            output_id,
            state: Arc::new(Mutex::new(ComponentState::Uninitialized)),
            mail_box: None,
            data_sender: None,
            control_sender: None,
        }
    }

    fn wait_with_retry(
        &self,
        flag: &TaskCompletionFlag,
        op: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let timeout = Duration::from_millis(CONTROL_OPERATION_TIMEOUT_MS);
        for retry in 0..CONTROL_OPERATION_MAX_RETRIES {
            match flag.wait_timeout(timeout) {
                Ok(_) => {
                    if let Some(err) = flag.get_error() {
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
            format!("{} failed after retries", op),
        )))
    }

    fn worker_loop(
        protocol: Arc<P>,
        data_rx: Receiver<BufferOrEvent>,
        control_rx: Receiver<OutputControlSignal>,
        control_tx: Sender<OutputControlSignal>,
        state: Arc<Mutex<ComponentState>>,
        mail_box: Option<Arc<ControlMailBox>>,
        output_id: usize,
    ) {
        use crossbeam_channel::select;
        let mut is_running = false;

        loop {
            if is_running {
                select! {
                    recv(control_rx) -> res => match res {
                        Ok(sig) => match Self::handle_signal(sig, &protocol, &data_rx, &control_tx, &state, mail_box.as_ref(), output_id) {
                            ControlAction::Continue => is_running = true,
                            ControlAction::Pause => is_running = false,
                            ControlAction::Exit => break,
                            ControlAction::Unchanged=> {}
                        },
                        Err(_) => break,
                    },
                    recv(data_rx) -> res => match res {
                        Ok(data) => {
                            if !Self::process_data(&protocol, data, &control_tx, mail_box.as_ref(), output_id) { break; }
                            for _ in 0..MAX_BATCH_CONSUME_SIZE {
                                match data_rx.try_recv() {
                                    Ok(more) => if !Self::process_data(&protocol, more, &control_tx, mail_box.as_ref(), output_id) { break; },
                                    Err(_) => break,
                                }
                            }
                            let _ = protocol.flush();
                        }
                        Err(_) => break,
                    }
                }
            } else {
                match control_rx.recv() {
                    Ok(sig) => match Self::handle_signal(
                        sig,
                        &protocol,
                        &data_rx,
                        &control_tx,
                        &state,
                        mail_box.as_ref(),
                        output_id,
                    ) {
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
        sig: OutputControlSignal,
        protocol: &Arc<P>,
        data_rx: &Receiver<BufferOrEvent>,
        control_tx: &Sender<OutputControlSignal>,
        state: &Arc<Mutex<ComponentState>>,
        mail_box: Option<&Arc<ControlMailBox>>,
        output_id: usize,
    ) -> ControlAction {
        let mut s = state.lock().unwrap();
        match sig {
            OutputControlSignal::Start { completion_flag } => {
                if let Err(e) = protocol.on_start() {
                    completion_flag.mark_error(e.to_string());
                    return ControlAction::Unchanged;
                }
                *s = ComponentState::Running;
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            OutputControlSignal::Stop { completion_flag } => {
                if let Err(e) = protocol.on_stop() {
                    completion_flag.mark_error(e.to_string());
                    return ControlAction::Unchanged;
                }
                *s = ComponentState::Stopped;
                completion_flag.mark_completed();
                ControlAction::Pause
            }
            OutputControlSignal::Close { completion_flag } => {
                *s = ComponentState::Closing;
                Self::drain_data(protocol, data_rx, control_tx, mail_box, output_id);
                let _ = protocol.on_close();
                *s = ComponentState::Closed;
                completion_flag.mark_completed();
                ControlAction::Exit
            }
            OutputControlSignal::Checkpoint {
                checkpoint_id,
                completion_flag,
            } => {
                Self::drain_data(protocol, data_rx, control_tx, mail_box, output_id);
                let _ = protocol.on_checkpoint(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            OutputControlSignal::CheckpointFinish {
                checkpoint_id,
                completion_flag,
            } => {
                let _ = protocol.on_checkpoint_finish(checkpoint_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            OutputControlSignal::Flush { completion_flag } => {
                Self::drain_data(protocol, data_rx, control_tx, mail_box, output_id);
                completion_flag.mark_completed();
                ControlAction::Continue
            }
            OutputControlSignal::Error {
                completion_flag,
                error,
            } => {
                *s = ComponentState::Error { error };
                completion_flag.mark_completed();
                ControlAction::Pause
            }
        }
    }

    fn drain_data(
        protocol: &Arc<P>,
        rx: &Receiver<BufferOrEvent>,
        tx: &Sender<OutputControlSignal>,
        mb: Option<&Arc<ControlMailBox>>,
        id: usize,
    ) {
        while let Ok(data) = rx.try_recv() {
            if !Self::process_data(protocol, data, tx, mb, id) {
                break;
            }
        }
        let _ = protocol.flush();
    }

    fn process_data(
        protocol: &Arc<P>,
        data: BufferOrEvent,
        tx: &Sender<OutputControlSignal>,
        mb: Option<&Arc<ControlMailBox>>,
        id: usize,
    ) -> bool {
        if let Err(e) = protocol.send(data) {
            let msg = e.to_string();
            let _ = tx.send(OutputControlSignal::Error {
                completion_flag: TaskCompletionFlag::new(),
                error: msg.clone(),
            });
            if let Some(m) = mb {
                let _ = m.send_error_report(FunctionErrorReport::output(id, msg));
            }
            return false;
        }
        true
    }
}

impl<P: OutputProtocol> Output for OutputRunner<P> {
    fn init_with_context(
        &mut self,
        ctx: &crate::runtime::taskexecutor::InitContext,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        if !matches!(*self.state.lock().unwrap(), ComponentState::Uninitialized) {
            return Ok(());
        }

        if let Some(ref m) = *ctx.get_control_mailbox().lock().unwrap() {
            self.mail_box = Some(Arc::clone(m));
        }

        self.protocol.init()?;

        let (d_s, d_r) = bounded(DEFAULT_CHANNEL_CAPACITY);
        let (c_s, c_r) = unbounded();
        self.data_sender = Some(d_s);
        self.control_sender = Some(c_s.clone());

        let protocol = self.protocol.clone();
        let state = self.state.clone();
        let mail_box = self.mail_box.clone();
        let output_id = self.output_id;

        let thread_handle = thread::Builder::new()
            .name(format!("output-runner-{}-{}", protocol.name(), self.output_id))
            .spawn(move || {
                Self::worker_loop(protocol, d_r, c_r, c_s, state, mail_box, output_id);
            })
            .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn std::error::Error + Send>)?;

        use crate::runtime::processor::wasm::thread_pool::{ThreadGroup, ThreadGroupType};
        let mut group = ThreadGroup::new(
            ThreadGroupType::Output(self.output_id),
            format!("Output-{}", self.output_id),
        );
        group.add_thread(thread_handle);
        ctx.register_thread_group(group);

        *self.state.lock().unwrap() = ComponentState::Initialized;
        Ok(())
    }

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        let sender = self.control_sender.as_ref().ok_or_else(|| {
            Box::new(std::io::Error::other("Not initialized")) as Box<dyn std::error::Error + Send>
        })?;
        sender
            .send(OutputControlSignal::Start {
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Start")
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        let sender = self.control_sender.as_ref().ok_or_else(|| {
            Box::new(std::io::Error::other("Not initialized")) as Box<dyn std::error::Error + Send>
        })?;
        sender
            .send(OutputControlSignal::Stop {
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Stop")
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        if let Some(ref sender) = self.control_sender {
            let _ = sender.send(OutputControlSignal::Close {
                completion_flag: flag.clone(),
            });
            let _ = self.wait_with_retry(&flag, "Close");
        }
        self.data_sender.take();
        self.control_sender.take();
        Ok(())
    }

    fn collect(&mut self, data: BufferOrEvent) -> Result<(), Box<dyn std::error::Error + Send>> {
        if let Some(ref sender) = self.data_sender {
            sender.send(data).map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e.to_string(),
                )) as Box<dyn std::error::Error + Send>
            })?;
        }
        Ok(())
    }

    fn take_checkpoint(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(OutputControlSignal::Checkpoint {
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
            .send(OutputControlSignal::CheckpointFinish {
                checkpoint_id: id,
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "CheckpointFinish")
    }

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(OutputControlSignal::Flush {
                completion_flag: flag.clone(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "Flush")
    }

    fn box_clone(&self) -> Box<dyn Output> {
        unimplemented!(
            "Runner cloning is not supported directly. Create a new instance with a cloned protocol."
        )
    }

    fn set_error_state(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let flag = TaskCompletionFlag::new();
        self.control_sender
            .as_ref()
            .unwrap()
            .send(OutputControlSignal::Error {
                completion_flag: flag.clone(),
                error: "Manual error".into(),
            })
            .unwrap();
        self.wait_with_retry(&flag, "SetErrorState")
    }
}
