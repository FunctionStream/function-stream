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
use tokio::sync::mpsc::Receiver;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{info, info_span, Instrument};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::network::endpoint::BoxedEventStream;
use crate::runtime::streaming::protocol::{
    control::{ControlCommand, StopMode},
    event::StreamEvent,
    stream_out::StreamOutput,
    tracked::TrackedEvent,
};
use crate::runtime::streaming::execution::tracker::{
    barrier_aligner::{AlignmentStatus, BarrierAligner},
    watermark_tracker::WatermarkTracker,
};
use crate::sql::common::{CheckpointBarrier, Watermark};

// ==========================================
// 第一部分：逻辑处理层 - 算子融合链 (Logical Driver)
// ==========================================

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

pub struct ChainedDriver {
    operator: Box<dyn MessageOperator>,
    next: Option<Box<dyn OperatorDrive>>,
}

impl ChainedDriver {
    pub fn new(operator: Box<dyn MessageOperator>, next: Option<Box<dyn OperatorDrive>>) -> Self {
        Self { operator, next }
    }

    /// 从后往前组装算子，构建责任链
    pub fn build_chain(mut operators: Vec<Box<dyn MessageOperator>>) -> Option<Box<dyn OperatorDrive>> {
        if operators.is_empty() {
            return None;
        }
        let mut next_driver: Option<Box<dyn OperatorDrive>> = None;
        while let Some(op) = operators.pop() {
            let current = ChainedDriver::new(op, next_driver);
            next_driver = Some(Box::new(current));
        }
        next_driver
    }

    async fn dispatch_outputs(
        &mut self,
        outputs: Vec<StreamOutput>,
        ctx: &mut TaskContext,
    ) -> Result<(), RunError> {
        for out in outputs {
            match out {
                StreamOutput::Forward(b) => {
                    if let Some(next) = &mut self.next {
                        next.process_event(0, TrackedEvent::control(StreamEvent::Data(b)), ctx)
                            .await?;
                    } else {
                        ctx.collect(b).await?;
                    }
                }
                StreamOutput::Keyed(hash, b) => {
                    if self.next.is_some() {
                        return Err(RunError::internal(format!(
                            "Topology Error: Keyed output emitted in the middle of chain by '{}'",
                            self.operator.name()
                        )));
                    }
                    ctx.collect_keyed(hash, b).await?;
                }
                StreamOutput::Broadcast(b) => {
                    if self.next.is_some() {
                        return Err(RunError::internal(format!(
                            "Topology Error: Broadcast output emitted in the middle of chain by '{}'",
                            self.operator.name()
                        )));
                    }
                    ctx.collect(b).await?;
                }
                StreamOutput::Watermark(wm) => {
                    if let Some(next) = &mut self.next {
                        next.process_event(
                            0,
                            TrackedEvent::control(StreamEvent::Watermark(wm)),
                            ctx,
                        )
                        .await?;
                    } else {
                        ctx.broadcast(StreamEvent::Watermark(wm)).await?;
                    }
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
        if let Some(next) = &mut self.next {
            next.process_event(0, TrackedEvent::control(event), ctx).await?;
        } else {
            match event {
                StreamEvent::Watermark(wm) => ctx.broadcast(StreamEvent::Watermark(wm)).await?,
                StreamEvent::Barrier(b) => ctx.broadcast(StreamEvent::Barrier(b)).await?,
                StreamEvent::EndOfStream => ctx.broadcast(StreamEvent::EndOfStream).await?,
                StreamEvent::Data(_) => unreachable!(),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl OperatorDrive for ChainedDriver {
    async fn on_start(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        self.operator.on_start(ctx).await?;
        if let Some(next) = &mut self.next {
            next.on_start(ctx).await?;
        }
        Ok(())
    }

    async fn process_event(
        &mut self,
        input_idx: usize,
        tracked: TrackedEvent,
        ctx: &mut TaskContext,
    ) -> Result<bool, RunError> {
        let mut should_stop = false;
        match tracked.event {
            StreamEvent::Data(batch) => {
                let outputs = self.operator.process_data(input_idx, batch, ctx).await?;
                self.dispatch_outputs(outputs, ctx).await?;
            }
            StreamEvent::Watermark(wm) => {
                let outputs = self.operator.process_watermark(wm.clone(), ctx).await?;
                self.dispatch_outputs(outputs, ctx).await?;
                self.forward_signal(StreamEvent::Watermark(wm), ctx).await?;
            }
            StreamEvent::Barrier(barrier) => {
                self.operator.snapshot_state(barrier.clone(), ctx).await?;
                self.forward_signal(StreamEvent::Barrier(barrier), ctx).await?;
            }
            StreamEvent::EndOfStream => {
                should_stop = true;
                self.forward_signal(StreamEvent::EndOfStream, ctx).await?;
            }
        }
        Ok(should_stop)
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
            ControlCommand::Stop { mode } => {
                if *mode == StopMode::Immediate {
                    stop = true;
                }
            }
            ControlCommand::DropState | ControlCommand::Start | ControlCommand::UpdateConfig { .. } => {}
        }

        if let Some(next) = &mut self.next {
            if next.handle_control(cmd, ctx).await? {
                stop = true;
            }
        } else if let ControlCommand::TriggerCheckpoint { barrier } = cmd {
            ctx.broadcast(StreamEvent::Barrier(barrier.into())).await?;
        }

        Ok(stop)
    }

    async fn on_close(&mut self, ctx: &mut TaskContext) -> Result<(), RunError> {
        let close_outs = self.operator.on_close(ctx).await?;
        self.dispatch_outputs(close_outs, ctx).await?;
        if let Some(next) = &mut self.next {
            next.on_close(ctx).await?;
        }
        Ok(())
    }
}

// ==========================================
// 第二部分：物理执行层 - 流水线 (Physical Driver)
// ==========================================

pub struct Pipeline {
    chain_head: Box<dyn OperatorDrive>,
    ctx: TaskContext,
    inboxes: Vec<BoxedEventStream>,
    control_rx: Receiver<ControlCommand>,

    wm_tracker: WatermarkTracker,
    barrier_aligner: BarrierAligner,
    /// Barrier 未对齐时从轮询池移除的输入流（背压）
    paused_streams: Vec<Option<BoxedEventStream>>,
}

impl Pipeline {
    pub fn new(
        operators: Vec<Box<dyn MessageOperator>>,
        ctx: TaskContext,
        inboxes: Vec<BoxedEventStream>,
        control_rx: Receiver<ControlCommand>,
    ) -> Result<Self, RunError> {
        let input_count = inboxes.len();
        let chain_head = ChainedDriver::build_chain(operators)
            .ok_or_else(|| RunError::internal("Cannot build pipeline with empty operators"))?;

        let paused_streams = (0..input_count).map(|_| None).collect();

        Ok(Self {
            chain_head,
            ctx,
            inboxes,
            control_rx,
            wm_tracker: WatermarkTracker::new(input_count),
            barrier_aligner: BarrierAligner::new(input_count),
            paused_streams,
        })
    }

    pub async fn run(mut self) -> Result<(), RunError> {
        let span = info_span!(
            "pipeline_run",
            job_id = %self.ctx.job_id,
            vertex = self.ctx.vertex_id
        );

        async move {
            info!("Pipeline initializing...");
            self.chain_head.on_start(&mut self.ctx).await?;

            let mut active_streams = StreamMap::new();
            for (i, stream) in std::mem::take(&mut self.inboxes).into_iter().enumerate() {
                active_streams.insert(i, stream);
            }

            loop {
                tokio::select! {
                    biased;

                    Some(cmd) = self.control_rx.recv() => {
                        if self.chain_head.handle_control(cmd, &mut self.ctx).await? {
                            break;
                        }
                    }

                    Some((idx, tracked_event)) = active_streams.next() => {
                        match tracked_event.event {
                            StreamEvent::Data(batch) => {
                                self.chain_head
                                    .process_event(
                                        idx,
                                        TrackedEvent::control(StreamEvent::Data(batch)),
                                        &mut self.ctx,
                                    )
                                    .await?;
                            }

                            StreamEvent::Barrier(barrier) => {
                                match self.barrier_aligner.mark(idx, &barrier) {
                                    AlignmentStatus::Pending => {
                                        if let Some(stream) = active_streams.remove(&idx) {
                                            self.paused_streams[idx] = Some(stream);
                                        }
                                    }
                                    AlignmentStatus::Complete => {
                                        self.chain_head
                                            .process_event(
                                                idx,
                                                TrackedEvent::control(StreamEvent::Barrier(barrier)),
                                                &mut self.ctx,
                                            )
                                            .await?;

                                        for i in 0..self.paused_streams.len() {
                                            if let Some(stream) = self.paused_streams[i].take() {
                                                active_streams.insert(i, stream);
                                            }
                                        }
                                    }
                                }
                            }

                            StreamEvent::Watermark(wm) => {
                                if let Some(aligned_wm) = self.wm_tracker.update(idx, wm) {
                                    if let Watermark::EventTime(t) = aligned_wm {
                                        self.ctx.advance_watermark(t);
                                    }
                                    self.chain_head
                                        .process_event(
                                            idx,
                                            TrackedEvent::control(StreamEvent::Watermark(aligned_wm)),
                                            &mut self.ctx,
                                        )
                                        .await?;
                                }
                            }

                            StreamEvent::EndOfStream => {
                                if self.wm_tracker.increment_eof() == self.wm_tracker.input_count() {
                                    self.chain_head
                                        .process_event(
                                            idx,
                                            TrackedEvent::control(StreamEvent::EndOfStream),
                                            &mut self.ctx,
                                        )
                                        .await?;
                                    break;
                                }
                            }
                        }
                    }

                    else => break,
                }
            }

            self.teardown().await
        }
        .instrument(span)
        .await
    }

    async fn teardown(mut self) -> Result<(), RunError> {
        info!("Pipeline tearing down...");
        self.chain_head.on_close(&mut self.ctx).await?;
        Ok(())
    }
}

/// 与执行引擎语义对齐的别名
pub type SubtaskRunner = Pipeline;
