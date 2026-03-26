use std::future::pending;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::memory::MemoryPool;
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use crate::runtime::streaming::storage::manager::TableManager;
use crate::sql::common::CheckpointBarrier;

pub struct PipelineRunner {
    chain: FusionOperatorChain,
    inbox: Option<mpsc::Receiver<TrackedEvent>>,
    outboxes: Vec<mpsc::Sender<TrackedEvent>>,
    control_rx: mpsc::Receiver<ControlCommand>,
    ctx: TaskContext,
}

impl PipelineRunner {
    pub fn new(
        pipeline_id: u32,
        chain: FusionOperatorChain,
        inbox: Option<mpsc::Receiver<TrackedEvent>>,
        outboxes: Vec<mpsc::Sender<TrackedEvent>>,
        control_rx: mpsc::Receiver<ControlCommand>,
        job_id: String,
        memory_pool: Arc<MemoryPool>,
        table_manager: Option<Arc<tokio::sync::Mutex<TableManager>>>,
    ) -> Self {
        Self {
            chain,
            inbox,
            outboxes,
            control_rx,
            ctx: TaskContext::new(job_id, pipeline_id, 0, 1, vec![], memory_pool, table_manager),
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.chain.on_start(&mut self.ctx).await?;

        'main: loop {
            tokio::select! {
                biased;
                Some(cmd) = self.control_rx.recv() => {
                    if self.handle_control(cmd).await? {
                        break 'main;
                    }
                }
                Some(event) = async {
                    if let Some(ref mut rx) = self.inbox { rx.recv().await }
                    else { pending().await }
                } => {
                    self.process_event(event).await?;
                }
            }
        }

        self.chain.on_close(&mut self.ctx).await?;
        Ok(())
    }

    async fn handle_control(&mut self, cmd: ControlCommand) -> anyhow::Result<bool> {
        match &cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let barrier: CheckpointBarrier = barrier.clone().into();
                self.chain.snapshot_state(barrier.clone(), &mut self.ctx).await?;
                self.broadcast(StreamEvent::Barrier(barrier)).await?;
            }
            ControlCommand::Commit { epoch } => {
                self.chain.commit_checkpoint(*epoch, &mut self.ctx).await?;
            }
            ControlCommand::Stop { mode } if *mode == StopMode::Immediate => {
                return Ok(true);
            }
            _ => {}
        }

        self.chain.handle_control(cmd, &mut self.ctx).await
    }

    async fn process_event(&mut self, tracked: TrackedEvent) -> anyhow::Result<()> {
        match tracked.event {
            StreamEvent::Data(batch) => {
                let outputs = self.chain.process_data(0, batch, &mut self.ctx).await?;
                self.emit_outputs(outputs).await?;
            }
            StreamEvent::Watermark(wm) => {
                let outputs = self.chain.process_watermark(wm.clone(), &mut self.ctx).await?;
                self.emit_outputs(outputs).await?;
                self.broadcast(StreamEvent::Watermark(wm)).await?;
            }
            StreamEvent::Barrier(barrier) => {
                self.chain.snapshot_state(barrier.clone(), &mut self.ctx).await?;
                self.broadcast(StreamEvent::Barrier(barrier)).await?;
            }
            StreamEvent::EndOfStream => {
                self.broadcast(StreamEvent::EndOfStream).await?;
            }
        }
        Ok(())
    }

    async fn emit_outputs(
        &mut self,
        outputs: Vec<crate::runtime::streaming::protocol::stream_out::StreamOutput>,
    ) -> anyhow::Result<()> {
        for out in outputs {
            match out {
                crate::runtime::streaming::protocol::stream_out::StreamOutput::Forward(batch)
                | crate::runtime::streaming::protocol::stream_out::StreamOutput::Broadcast(batch)
                | crate::runtime::streaming::protocol::stream_out::StreamOutput::Keyed(_, batch) => {
                    self.broadcast(StreamEvent::Data(batch)).await?;
                }
                crate::runtime::streaming::protocol::stream_out::StreamOutput::Watermark(wm) => {
                    self.broadcast(StreamEvent::Watermark(wm)).await?;
                }
            }
        }
        Ok(())
    }

    async fn broadcast(&self, event: StreamEvent) -> anyhow::Result<()> {
        let tracked = TrackedEvent::control(event);
        for tx in &self.outboxes {
            tx.send(tracked.clone()).await?;
        }
        Ok(())
    }
}

pub struct FusionOperatorChain {
    operators: Vec<Box<dyn MessageOperator>>,
}

impl FusionOperatorChain {
    pub fn new(operators: Vec<Box<dyn MessageOperator>>) -> Self {
        Self { operators }
    }

    pub async fn on_start(&mut self, ctx: &mut TaskContext) -> anyhow::Result<()> {
        for op in &mut self.operators {
            op.on_start(ctx).await?;
        }
        Ok(())
    }

    pub async fn process_data(
        &mut self,
        input_idx: usize,
        batch: arrow_array::RecordBatch,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<crate::runtime::streaming::protocol::stream_out::StreamOutput>> {
        let mut data_batches = vec![batch];
        for (idx, op) in self.operators.iter_mut().enumerate() {
            let mut next_batches = Vec::new();
            for b in data_batches {
                let outputs = op
                    .process_data(if idx == 0 { input_idx } else { 0 }, b, ctx)
                    .await?;
                for out in outputs {
                    match out {
                        crate::runtime::streaming::protocol::stream_out::StreamOutput::Forward(b)
                        | crate::runtime::streaming::protocol::stream_out::StreamOutput::Broadcast(b)
                        | crate::runtime::streaming::protocol::stream_out::StreamOutput::Keyed(_, b) => {
                            next_batches.push(b);
                        }
                        crate::runtime::streaming::protocol::stream_out::StreamOutput::Watermark(_) => {}
                    }
                }
            }
            data_batches = next_batches;
        }
        Ok(data_batches
            .into_iter()
            .map(crate::runtime::streaming::protocol::stream_out::StreamOutput::Forward)
            .collect())
    }

    pub async fn process_watermark(
        &mut self,
        watermark: crate::sql::common::Watermark,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<Vec<crate::runtime::streaming::protocol::stream_out::StreamOutput>> {
        let mut outs = vec![crate::runtime::streaming::protocol::stream_out::StreamOutput::Watermark(watermark)];
        for op in &mut self.operators {
            let mut next = Vec::new();
            for out in outs {
                match out {
                    crate::runtime::streaming::protocol::stream_out::StreamOutput::Watermark(wm) => {
                        let mut produced = op.process_watermark(wm, ctx).await?;
                        next.append(&mut produced);
                    }
                    other => next.push(other),
                }
            }
            outs = next;
        }
        Ok(outs)
    }

    pub async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<()> {
        for op in &mut self.operators {
            op.snapshot_state(barrier.clone(), ctx).await?;
        }
        Ok(())
    }

    pub async fn commit_checkpoint(&mut self, epoch: u32, ctx: &mut TaskContext) -> anyhow::Result<()> {
        for op in &mut self.operators {
            op.commit_checkpoint(epoch, ctx).await?;
        }
        Ok(())
    }

    pub async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        ctx: &mut TaskContext,
    ) -> anyhow::Result<bool> {
        let mut should_stop = false;
        for op in &mut self.operators {
            should_stop = should_stop || op.handle_control(cmd.clone(), ctx).await?;
        }
        Ok(should_stop)
    }

    pub async fn on_close(&mut self, ctx: &mut TaskContext) -> anyhow::Result<()> {
        for op in &mut self.operators {
            let _ = op.on_close(ctx).await?;
        }
        Ok(())
    }
}
