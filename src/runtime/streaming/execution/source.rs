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

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{SourceEvent, SourceOperator};
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::execution::runner::OperatorDrive;
use crate::runtime::streaming::protocol::control::ControlCommand;
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::event::TrackedEvent;
use crate::sql::common::CheckpointBarrier;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{Instrument, info, info_span, warn};

pub const SOURCE_IDLE_SLEEP: Duration = Duration::from_millis(50);
pub const WATERMARK_EMIT_INTERVAL: Duration = Duration::from_millis(200);

pub struct SourceRunner {
    operator: Box<dyn SourceOperator>,
    chain_head: Option<Box<dyn OperatorDrive>>,
    ctx: TaskContext,
    control_rx: Receiver<ControlCommand>,
}

impl SourceRunner {
    pub fn new(
        operator: Box<dyn SourceOperator>,
        chain_head: Option<Box<dyn OperatorDrive>>,
        ctx: TaskContext,
        control_rx: Receiver<ControlCommand>,
    ) -> Self {
        Self {
            operator,
            chain_head,
            ctx,
            control_rx,
        }
    }

    pub async fn run(mut self) -> Result<(), RunError> {
        let span = info_span!(
            "source_run",
            vertex = self.ctx.vertex_id,
            op = self.operator.name()
        );

        async move {
            info!("Source subtask starting");
            self.operator.on_start(&mut self.ctx).await?;
            if let Some(chain) = &mut self.chain_head {
                chain.on_start(&mut self.ctx).await?;
            }

            let mut idle_timer = interval(SOURCE_IDLE_SLEEP);
            idle_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut wm_timer = interval(WATERMARK_EMIT_INTERVAL);
            wm_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut is_idle = false;
            let mut is_running = true;

            while is_running {
                tokio::select! {
                    biased;

                    cmd_opt = self.control_rx.recv() => {
                        match cmd_opt {
                            None => is_running = false,
                            Some(cmd) => {
                                if self.handle_control(cmd).await? {
                                    is_running = false;
                                }
                            }
                        }
                    }

                    _ = wm_timer.tick() => {
                        if let Some(wm) = self.operator.poll_watermark() {
                            self.dispatch_event(StreamEvent::Watermark(wm)).await?;
                        }
                    }

                    _ = idle_timer.tick(), if is_idle => {
                        is_idle = false;
                    }

                    fetch_res = self.operator.fetch_next(&mut self.ctx), if !is_idle => {
                        match fetch_res {
                            Ok(SourceEvent::Data(batch)) => {
                                self.dispatch_event(StreamEvent::Data(batch)).await?;
                            }
                            Ok(SourceEvent::Watermark(wm)) => {
                                self.dispatch_event(StreamEvent::Watermark(wm)).await?;
                            }
                            Ok(SourceEvent::Idle) => {
                                is_idle = true;
                                idle_timer.reset();
                            }
                            Ok(SourceEvent::EndOfStream) => {
                                self.dispatch_event(StreamEvent::EndOfStream).await?;
                                is_running = false;
                            }
                            Err(e) => {
                                warn!("fetch_next error: {}", e);
                                return Err(RunError::Operator(e));
                            }
                        }
                    }
                }
            }

            self.teardown().await
        }
        .instrument(span)
        .await
    }

    async fn dispatch_event(&mut self, event: StreamEvent) -> Result<(), RunError> {
        if let Some(chain) = &mut self.chain_head {
            let _stop = chain
                .process_event(0, TrackedEvent::control(event), &mut self.ctx)
                .await?;
        } else {
            match event {
                StreamEvent::Data(b) => self.ctx.collect(b).await?,
                StreamEvent::Watermark(w) => {
                    self.ctx.broadcast(StreamEvent::Watermark(w)).await?;
                }
                StreamEvent::Barrier(b) => {
                    self.ctx.broadcast(StreamEvent::Barrier(b)).await?;
                }
                StreamEvent::EndOfStream => {
                    self.ctx.broadcast(StreamEvent::EndOfStream).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_control(&mut self, cmd: ControlCommand) -> Result<bool, RunError> {
        match cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let b: CheckpointBarrier = barrier.into();
                self.operator
                    .snapshot_state(b.clone(), &mut self.ctx)
                    .await?;
                self.dispatch_event(StreamEvent::Barrier(b)).await?;
            }
            ControlCommand::Stop { .. } => return Ok(true),
            other => {
                if let Some(chain) = &mut self.chain_head {
                    if chain.handle_control(other, &mut self.ctx).await? {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    async fn teardown(mut self) -> Result<(), RunError> {
        self.operator.on_close(&mut self.ctx).await?;
        if let Some(chain) = &mut self.chain_head {
            chain.on_close(&mut self.ctx).await?;
        }
        info!("Source subtask shutdown");
        Ok(())
    }
}
