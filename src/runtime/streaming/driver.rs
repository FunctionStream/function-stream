use std::future::pending;
use std::sync::Arc;

use arrow_array::RecordBatch;
use tokio::sync::mpsc;

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::{MessageOperator, OperatorContext, StreamOperator};
use crate::runtime::streaming::context::{ChainedOperatorContext, TerminalOutputContext};
use crate::runtime::streaming::environment::TaskEnvironment;
use crate::runtime::streaming::protocol::control::{ControlCommand, StopMode};
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::stream_out::StreamOutput;
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use crate::sql::common::CheckpointBarrier;

pub struct StreamTaskDriver {
    head_op: Box<dyn StreamOperator>,
    head_ctx: Box<dyn OperatorContext>,
    inbox: Option<mpsc::Receiver<TrackedEvent>>,
    control_rx: mpsc::Receiver<ControlCommand>,
}

impl StreamTaskDriver {
    pub fn new(
        task_id: u32,
        mut operators: Vec<Box<dyn StreamOperator>>,
        inbox: Option<mpsc::Receiver<TrackedEvent>>,
        outboxes: Vec<mpsc::Sender<TrackedEvent>>,
        control_rx: mpsc::Receiver<ControlCommand>,
        job_id: String,
    ) -> Self {
        let env = TaskEnvironment::new(job_id, task_id, 0, 1);
        let mut current_op = operators.pop().expect("Operators pipeline cannot be empty");
        let mut current_ctx: Box<dyn OperatorContext> =
            Box::new(TerminalOutputContext::new(outboxes, env));

        while let Some(prev_op) = operators.pop() {
            let chained = ChainedOperatorContext::new(current_op, current_ctx);
            current_op = prev_op;
            current_ctx = Box::new(chained);
        }

        Self {
            head_op: current_op,
            head_ctx: current_ctx,
            inbox,
            control_rx,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.head_op.open(self.head_ctx.env()).await?;

        'main_loop: loop {
            tokio::select! {
                biased;
                Some(cmd) = self.control_rx.recv() => {
                    if self.process_control_command(cmd).await? {
                        break 'main_loop;
                    }
                }
                Some(tracked) = async {
                    if let Some(ref mut rx) = self.inbox { rx.recv().await }
                    else { pending().await }
                } => {
                    self.pump_event(tracked.event).await?;
                }
            }
        }

        self.head_op.close(self.head_ctx.env()).await?;
        Ok(())
    }

    async fn process_control_command(&mut self, cmd: ControlCommand) -> anyhow::Result<bool> {
        match cmd {
            ControlCommand::TriggerCheckpoint { barrier } => {
                let barrier: CheckpointBarrier = barrier.into();
                self.pump_event(StreamEvent::Barrier(barrier)).await?;
                Ok(false)
            }
            ControlCommand::Commit { epoch } => {
                self.head_op.commit_checkpoint(epoch, self.head_ctx.env()).await?;
                self.head_ctx.commit_checkpoint(epoch).await?;
                Ok(false)
            }
            ControlCommand::Stop { mode } if mode == StopMode::Immediate => Ok(true),
            other_cmd => {
                let stop_head = self
                    .head_op
                    .handle_control(other_cmd.clone(), self.head_ctx.env())
                    .await?;
                let stop_rest = self.head_ctx.handle_control(other_cmd).await?;
                Ok(stop_head || stop_rest)
            }
        }
    }

    async fn pump_event(&mut self, event: StreamEvent) -> anyhow::Result<()> {
        match event {
            StreamEvent::Data(batch) => self.head_op.process_data(batch, self.head_ctx.as_mut()).await,
            StreamEvent::Watermark(wm) => {
                self.head_op.process_watermark(wm, self.head_ctx.as_mut()).await
            }
            StreamEvent::Barrier(br) => {
                self.head_op
                    .snapshot_state(br.clone(), self.head_ctx.as_mut())
                    .await?;
                self.head_ctx.broadcast(StreamEvent::Barrier(br)).await
            }
            StreamEvent::EndOfStream => {
                self.head_op.close(self.head_ctx.env()).await?;
                self.head_ctx.broadcast(StreamEvent::EndOfStream).await
            }
        }
    }
}

pub struct MessageOperatorAdapter {
    inner: Box<dyn MessageOperator>,
}

impl MessageOperatorAdapter {
    pub fn new(inner: Box<dyn MessageOperator>) -> Self {
        Self { inner }
    }

    async fn emit_outputs(
        ctx: &mut dyn OperatorContext,
        outputs: Vec<StreamOutput>,
    ) -> anyhow::Result<()> {
        for out in outputs {
            match out {
                StreamOutput::Forward(b) | StreamOutput::Broadcast(b) | StreamOutput::Keyed(_, b) => {
                    ctx.collect(b).await?;
                }
                StreamOutput::Watermark(wm) => {
                    ctx.broadcast(StreamEvent::Watermark(wm)).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl StreamOperator for MessageOperatorAdapter {
    async fn open(&mut self, env: &mut TaskEnvironment) -> anyhow::Result<()> {
        let mut ctx = TaskContext::new(
            env.job_id.clone(),
            env.task_id,
            env.subtask_index,
            env.parallelism,
            vec![],
            env.memory_pool.clone(),
        );
        self.inner.on_start(&mut ctx).await
    }

    async fn close(&mut self, env: &mut TaskEnvironment) -> anyhow::Result<()> {
        let mut ctx = TaskContext::new(
            env.job_id.clone(),
            env.task_id,
            env.subtask_index,
            env.parallelism,
            vec![],
            env.memory_pool.clone(),
        );
        let _ = self.inner.on_close(&mut ctx).await?;
        Ok(())
    }

    async fn process_data(
        &mut self,
        batch: RecordBatch,
        ctx: &mut dyn OperatorContext,
    ) -> anyhow::Result<()> {
        let mut op_ctx = TaskContext::new(
            ctx.env().job_id.clone(),
            ctx.env().task_id,
            ctx.env().subtask_index,
            ctx.env().parallelism,
            vec![],
            ctx.env().memory_pool.clone(),
        );
        let outs = self.inner.process_data(0, batch, &mut op_ctx).await?;
        Self::emit_outputs(ctx, outs).await
    }

    async fn process_watermark(
        &mut self,
        wm: crate::sql::common::Watermark,
        ctx: &mut dyn OperatorContext,
    ) -> anyhow::Result<()> {
        let mut op_ctx = TaskContext::new(
            ctx.env().job_id.clone(),
            ctx.env().task_id,
            ctx.env().subtask_index,
            ctx.env().parallelism,
            vec![],
            ctx.env().memory_pool.clone(),
        );
        let outs = self.inner.process_watermark(wm, &mut op_ctx).await?;
        Self::emit_outputs(ctx, outs).await
    }

    async fn snapshot_state(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut dyn OperatorContext,
    ) -> anyhow::Result<()> {
        let mut op_ctx = TaskContext::new(
            ctx.env().job_id.clone(),
            ctx.env().task_id,
            ctx.env().subtask_index,
            ctx.env().parallelism,
            vec![],
            ctx.env().memory_pool.clone(),
        );
        self.inner.snapshot_state(barrier, &mut op_ctx).await
    }

    async fn commit_checkpoint(
        &mut self,
        epoch: u32,
        env: &mut TaskEnvironment,
    ) -> anyhow::Result<()> {
        let mut ctx = TaskContext::new(
            env.job_id.clone(),
            env.task_id,
            env.subtask_index,
            env.parallelism,
            vec![],
            env.memory_pool.clone(),
        );
        self.inner.commit_checkpoint(epoch, &mut ctx).await
    }

    async fn handle_control(
        &mut self,
        cmd: ControlCommand,
        _env: &mut TaskEnvironment,
    ) -> anyhow::Result<bool> {
        match cmd {
            ControlCommand::Stop { mode } => Ok(mode == StopMode::Immediate),
            ControlCommand::DropState
            | ControlCommand::Start
            | ControlCommand::UpdateConfig { .. }
            | ControlCommand::TriggerCheckpoint { .. }
            | ControlCommand::Commit { .. } => Ok(false),
        }
    }
}
