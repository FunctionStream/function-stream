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

use async_trait::async_trait;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{Collector, Operator};
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::protocol::{
    control::{ControlCommand, StopMode},
    event::{StreamEvent, StreamOutput, TrackedEvent},
};
use crate::sql::common::CheckpointBarrier;

#[async_trait]
pub trait OperatorDrive: Send {
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<(), RunError>;
    async fn process_event(
        &mut self,
        input_idx: usize,
        event: TrackedEvent,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError>;
    async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError>;
    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<(), RunError>;
}

pub struct ChainBuilder;

impl ChainBuilder {
    pub fn build(mut operators: Vec<Box<dyn Operator>>) -> Option<Box<dyn OperatorDrive>> {
        let tail_operator = operators.pop()?;

        let mut current_driver: Box<dyn OperatorDrive> = Box::new(TailDriver::new(tail_operator));

        while let Some(op) = operators.pop() {
            current_driver = Box::new(IntermediateDriver::new(op, current_driver));
        }

        Some(current_driver)
    }
}

pub struct IntermediateDriver {
    operator: Box<dyn Operator>,
    next: Box<dyn OperatorDrive>,
}

impl IntermediateDriver {
    pub fn new(operator: Box<dyn Operator>, next: Box<dyn OperatorDrive>) -> Self {
        Self { operator, next }
    }

    async fn dispatch_outputs(
        &mut self,
        outputs: Vec<StreamOutput>,
        ctx: &mut TaskContext,
    ) -> Result<(), RunError> {
        for out in outputs {
            match out {
                StreamOutput::Forward(b) => {
                    self.next
                        .process_event(0, TrackedEvent::control(StreamEvent::Data(b)), ctx)
                        .await?;
                }
                StreamOutput::Watermark(wm) => {
                    self.next
                        .process_event(0, TrackedEvent::control(StreamEvent::Watermark(wm)), ctx)
                        .await?;
                }
                StreamOutput::Keyed(_, _) | StreamOutput::Broadcast(_) => {
                    return Err(RunError::internal(format!(
                        "Topology Violation: Keyed or Broadcast output emitted in the middle of chain by '{}'",
                        self.operator.name()
                    )));
                }
            }
        }
        Ok(())
    }

    async fn forward_signal(
        &mut self,
        event: StreamEvent,
        ctx: &mut TaskContext,
    ) -> Result<(), RunError> {
        self.next
            .process_event(0, TrackedEvent::control(event), ctx)
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl OperatorDrive for IntermediateDriver {
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        self.operator.on_start(ctx).await?;
        self.next.on_start(ctx).await?;
        Ok(())
    }

    async fn process_event(
        &mut self,
        input_idx: usize,
        tracked: TrackedEvent,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError> {
        match tracked.event {
            StreamEvent::Data(batch) => {
                struct NextCollector<'a> {
                    next: &'a mut Box<dyn OperatorDrive>,
                    op_name: String,
                }
                #[async_trait]
                impl Collector for NextCollector<'_> {
                    async fn collect(
                        &mut self,
                        out: StreamOutput,
                        ctx: &mut TaskContext,
                    ) -> anyhow::Result<()> {
                        match out {
                            StreamOutput::Forward(b) => {
                                self.next
                                    .process_event(
                                        0,
                                        TrackedEvent::control(StreamEvent::Data(b)),
                                        ctx,
                                    )
                                    .await?;
                            }
                            StreamOutput::Watermark(wm) => {
                                self.next
                                    .process_event(
                                        0,
                                        TrackedEvent::control(StreamEvent::Watermark(wm)),
                                        ctx,
                                    )
                                    .await?;
                            }
                            StreamOutput::Keyed(_, _) | StreamOutput::Broadcast(_) => {
                                return Err(anyhow::anyhow!(
                                    "Topology Violation: Keyed or Broadcast output emitted in the middle of chain by '{}'",
                                    self.op_name
                                ));
                            }
                        }
                        Ok(())
                    }
                }
                let mut collector = NextCollector {
                    next: &mut self.next,
                    op_name: self.operator.name().to_string(),
                };
                self.operator
                    .process_data(input_idx, batch, ctx, &mut collector)
                    .await?;
                Ok(false)
            }
            StreamEvent::Watermark(wm) => {
                struct NextCollector<'a> {
                    next: &'a mut Box<dyn OperatorDrive>,
                    op_name: String,
                }
                #[async_trait]
                impl Collector for NextCollector<'_> {
                    async fn collect(
                        &mut self,
                        out: StreamOutput,
                        ctx: &mut TaskContext,
                    ) -> anyhow::Result<()> {
                        match out {
                            StreamOutput::Forward(b) => {
                                self.next
                                    .process_event(
                                        0,
                                        TrackedEvent::control(StreamEvent::Data(b)),
                                        ctx,
                                    )
                                    .await?;
                            }
                            StreamOutput::Watermark(wm) => {
                                self.next
                                    .process_event(
                                        0,
                                        TrackedEvent::control(StreamEvent::Watermark(wm)),
                                        ctx,
                                    )
                                    .await?;
                            }
                            StreamOutput::Keyed(_, _) | StreamOutput::Broadcast(_) => {
                                return Err(anyhow::anyhow!(
                                    "Topology Violation: Keyed or Broadcast output emitted in the middle of chain by '{}'",
                                    self.op_name
                                ));
                            }
                        }
                        Ok(())
                    }
                }
                let mut collector = NextCollector {
                    next: &mut self.next,
                    op_name: self.operator.name().to_string(),
                };
                self.operator
                    .process_watermark(wm, ctx, &mut collector)
                    .await?;
                self.forward_signal(StreamEvent::Watermark(wm), ctx).await?;
                Ok(false)
            }
            StreamEvent::Barrier(barrier) => {
                self.operator.snapshot_state(barrier, ctx).await?;
                self.forward_signal(StreamEvent::Barrier(barrier), ctx)
                    .await?;
                Ok(false)
            }
            StreamEvent::EndOfStream => {
                self.forward_signal(StreamEvent::EndOfStream, ctx).await?;
                Ok(true)
            }
        }
    }

    async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError> {
        let mut stop = false;

        match &cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let b: CheckpointBarrier = barrier.clone().into();
                self.operator.snapshot_state(b, ctx).await?;
            }
            ControlCommand::Commit { epoch } => {
                self.operator.commit_checkpoint(*epoch, ctx).await?;
            }
            ControlCommand::AbortCheckpoint { epoch } => {
                self.operator.abort_checkpoint(*epoch, ctx).await?;
            }
            ControlCommand::Stop { mode } if *mode == StopMode::Immediate => {
                stop = true;
            }
            _ => {}
        }

        if self.next.handle_control(cmd, ctx).await? {
            stop = true;
        }

        Ok(stop)
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        let close_outs = self.operator.on_close(ctx).await?;
        self.dispatch_outputs(close_outs, ctx).await?;
        self.next.on_close(ctx).await?;
        Ok(())
    }
}

pub struct TailDriver {
    operator: Box<dyn Operator>,
}

impl TailDriver {
    pub fn new(operator: Box<dyn Operator>) -> Self {
        Self { operator }
    }

    async fn dispatch_outputs(
        &mut self,
        outputs: Vec<StreamOutput>,
        ctx: &mut TaskContext,
    ) -> Result<(), RunError> {
        for out in outputs {
            match out {
                StreamOutput::Forward(b) => ctx.collect(b).await?,
                StreamOutput::Keyed(hash, b) => ctx.collect_keyed(hash, b).await?,
                StreamOutput::Broadcast(b) => ctx.collect(b).await?,
                StreamOutput::Watermark(wm) => ctx.broadcast(StreamEvent::Watermark(wm)).await?,
            }
        }
        Ok(())
    }

    async fn forward_signal(
        &mut self,
        event: StreamEvent,
        ctx: &mut TaskContext,
    ) -> Result<(), RunError> {
        match event {
            StreamEvent::Watermark(wm) => ctx.broadcast(StreamEvent::Watermark(wm)).await?,
            StreamEvent::Barrier(b) => ctx.broadcast(StreamEvent::Barrier(b)).await?,
            StreamEvent::EndOfStream => ctx.broadcast(StreamEvent::EndOfStream).await?,
            StreamEvent::Data(_) => unreachable!("Data signal should not be forwarded implicitly"),
        }
        Ok(())
    }
}

#[async_trait]
impl OperatorDrive for TailDriver {
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        self.operator.on_start(ctx).await?;
        Ok(())
    }

    async fn process_event(
        &mut self,
        input_idx: usize,
        tracked: TrackedEvent,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError> {
        match tracked.event {
            StreamEvent::Data(batch) => {
                struct FinalCollector;
                #[async_trait]
                impl Collector for FinalCollector {
                    async fn collect(
                        &mut self,
                        out: StreamOutput,
                        ctx: &mut TaskContext,
                    ) -> anyhow::Result<()> {
                        match out {
                            StreamOutput::Forward(b) => ctx.collect(b).await?,
                            StreamOutput::Keyed(hash, b) => ctx.collect_keyed(hash, b).await?,
                            StreamOutput::Broadcast(b) => ctx.collect(b).await?,
                            StreamOutput::Watermark(wm) => {
                                ctx.broadcast(StreamEvent::Watermark(wm)).await?
                            }
                        }
                        Ok(())
                    }
                }
                let mut collector = FinalCollector;
                self.operator
                    .process_data(input_idx, batch, ctx, &mut collector)
                    .await?;
                Ok(false)
            }
            StreamEvent::Watermark(wm) => {
                struct FinalCollector;
                #[async_trait]
                impl Collector for FinalCollector {
                    async fn collect(
                        &mut self,
                        out: StreamOutput,
                        ctx: &mut TaskContext,
                    ) -> anyhow::Result<()> {
                        match out {
                            StreamOutput::Forward(b) => ctx.collect(b).await?,
                            StreamOutput::Keyed(hash, b) => ctx.collect_keyed(hash, b).await?,
                            StreamOutput::Broadcast(b) => ctx.collect(b).await?,
                            StreamOutput::Watermark(wm) => {
                                ctx.broadcast(StreamEvent::Watermark(wm)).await?
                            }
                        }
                        Ok(())
                    }
                }
                let mut collector = FinalCollector;
                self.operator
                    .process_watermark(wm, ctx, &mut collector)
                    .await?;
                self.forward_signal(StreamEvent::Watermark(wm), ctx).await?;
                Ok(false)
            }
            StreamEvent::Barrier(barrier) => {
                self.operator.snapshot_state(barrier, ctx).await?;
                self.forward_signal(StreamEvent::Barrier(barrier), ctx)
                    .await?;
                Ok(false)
            }
            StreamEvent::EndOfStream => {
                self.forward_signal(StreamEvent::EndOfStream, ctx).await?;
                Ok(true)
            }
        }
    }

    async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError> {
        let mut stop = false;

        match &cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let b: CheckpointBarrier = barrier.clone().into();
                self.operator.snapshot_state(b, ctx).await?;
                ctx.broadcast(StreamEvent::Barrier(b)).await?;
            }
            ControlCommand::Commit { epoch } => {
                self.operator.commit_checkpoint(*epoch, ctx).await?;
            }
            ControlCommand::AbortCheckpoint { epoch } => {
                self.operator.abort_checkpoint(*epoch, ctx).await?;
            }
            ControlCommand::Stop { mode } if *mode == StopMode::Immediate => {
                stop = true;
            }
            _ => {}
        }

        Ok(stop)
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        let close_outs = self.operator.on_close(ctx).await?;
        self.dispatch_outputs(close_outs, ctx).await?;
        Ok(())
    }
}
