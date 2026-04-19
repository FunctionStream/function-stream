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

use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, sleep};
use tracing::{Instrument, info, info_span, warn};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{SourceCheckpointReport, SourceEvent, SourceOperator};
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::execution::OperatorDrive;
use crate::runtime::streaming::protocol::{
    control::ControlCommand,
    event::{StreamEvent, TrackedEvent},
};
use crate::sql::common::CheckpointBarrier;

pub struct SourceDriver {
    operator: Box<dyn SourceOperator>,
    chain_head: Option<Box<dyn OperatorDrive>>,
    ctx: TaskContext,
    control_rx: Receiver<ControlCommand>,
}

impl SourceDriver {
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
            job_id = %self.ctx.job_id,
            pipeline_id = self.ctx.pipeline_id,
            op = self.operator.name()
        );

        async move {
            info!("SourceDriver initializing...");

            self.operator.on_start(&mut self.ctx).await?;
            if let Some(chain) = &mut self.chain_head {
                chain.on_start(&mut self.ctx).await?;
            }

            let idle_timeout = self.ctx.config().source_idle_timeout;

            let idle_delay = sleep(idle_timeout);
            tokio::pin!(idle_delay);

            let mut is_idle = false;

            loop {
                tokio::select! {
                    biased;

                    Some(cmd) = self.control_rx.recv() => {
                        if self.handle_control(cmd).await? {
                            info!("SourceDriver received stop signal, breaking event loop.");
                            break;
                        }
                    }

                    () = idle_delay.as_mut(), if is_idle => {
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
                                idle_delay
                                    .as_mut()
                                    .reset(Instant::now() + idle_timeout);
                            }
                            Ok(SourceEvent::EndOfStream) => {
                                self.dispatch_event(StreamEvent::EndOfStream).await?;
                                info!(
                                    "Source '{}' reached EndOfStream, pipeline shutting down gracefully.",
                                    self.operator.name()
                                );
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Source operator '{}' encountered critical fetch error: {}",
                                    self.operator.name(),
                                    e
                                );
                                return Err(RunError::Operator(e));
                            }
                        }
                    }

                    else => {
                        warn!("Control channel closed unexpectedly, SourceDriver shutting down.");
                        break;
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
            chain
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
        let mut stop = false;
        let mut pending_source_checkpoint: Option<(u64, SourceCheckpointReport)> = None;

        match &cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let b: CheckpointBarrier = barrier.clone().into();
                let report = self.operator.snapshot_state(b, &mut self.ctx).await?;
                self.dispatch_event(StreamEvent::Barrier(b)).await?;
                pending_source_checkpoint = Some((b.epoch as u64, report));
            }
            ControlCommand::Commit { epoch } => {
                self.operator
                    .commit_checkpoint(*epoch, &mut self.ctx)
                    .await?;
            }
            ControlCommand::AbortCheckpoint { epoch } => {
                self.operator
                    .abort_checkpoint(*epoch, &mut self.ctx)
                    .await?;
            }
            ControlCommand::Stop { .. } => {
                stop = true;
            }
            _ => {}
        }

        if let Some(chain) = &mut self.chain_head
            && chain.handle_control(cmd, &mut self.ctx).await?
        {
            stop = true;
        }

        if let Some((epoch, report)) = pending_source_checkpoint {
            self.ctx
                .send_checkpoint_ack(epoch, report.payloads)
                .await;
        }

        Ok(stop)
    }

    async fn teardown(mut self) -> Result<(), RunError> {
        info!("SourceDriver teardown initiated...");
        self.operator.on_close(&mut self.ctx).await?;
        if let Some(chain) = &mut self.chain_head {
            chain.on_close(&mut self.ctx).await?;
        }
        info!("SourceDriver teardown complete. Goodbye.");
        Ok(())
    }
}
