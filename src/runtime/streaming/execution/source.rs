use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::source::{SourceEvent, SourceOperator};
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::protocol::control::ControlCommand;
use crate::runtime::streaming::protocol::event::StreamEvent;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use crate::sql::common::CheckpointBarrier;

pub const SOURCE_IDLE_SLEEP: Duration = Duration::from_millis(50);

pub struct SourceRunner {
    operator: Box<dyn SourceOperator>,
    ctx: TaskContext,
    control_rx: Receiver<ControlCommand>,
}

impl SourceRunner {
    pub fn new(
        operator: Box<dyn SourceOperator>,
        ctx: TaskContext,
        control_rx: Receiver<ControlCommand>,
    ) -> Self {
        Self {
            operator,
            ctx,
            control_rx,
        }
    }

    pub async fn run(mut self) -> Result<(), RunError> {
        info!(
            job_id = %self.ctx.job_id,
            vertex = self.ctx.vertex_id,
            subtask = self.ctx.subtask_idx,
            operator = %self.operator.name(),
            "source subtask starting"
        );

        self.operator.on_start(&mut self.ctx).await?;

        let mut is_running = true;
        let mut idle_pending = false;

        while is_running {
            tokio::select! {
                biased;
                cmd_opt = self.control_rx.recv() => {
                    match cmd_opt {
                        None => {
                            debug!(
                                vertex = self.ctx.vertex_id,
                                subtask = self.ctx.subtask_idx,
                                "source control channel closed"
                            );
                            is_running = false;
                        }
                        Some(cmd) => {
                            match cmd {
                                ControlCommand::Stop { .. } => {
                                    is_running = false;
                                }
                                ControlCommand::TriggerCheckpoint { barrier } => {
                                    let barrier: CheckpointBarrier = barrier.into();
                                    self.operator
                                        .snapshot_state(barrier, &mut self.ctx)
                                        .await?;
                                    self.ctx
                                        .broadcast(StreamEvent::Barrier(barrier))
                                        .await?;
                                }
                                ControlCommand::Start
                                | ControlCommand::DropState
                                | ControlCommand::Commit { .. }
                                | ControlCommand::UpdateConfig { .. } => {
                                    debug!(?cmd, "source: ignored control command");
                                }
                            }
                        }
                    }
                }
                _ = sleep(SOURCE_IDLE_SLEEP), if is_running && idle_pending => {
                    idle_pending = false;
                }
                fetch_res = self.operator.fetch_next(&mut self.ctx), if is_running && !idle_pending => {
                    match fetch_res {
                        Ok(SourceEvent::Data(batch)) => {
                            self.ctx.collect(batch).await?;
                        }
                        Ok(SourceEvent::Watermark(wm)) => {
                            self.ctx.broadcast(StreamEvent::Watermark(wm)).await?;
                        }
                        Ok(SourceEvent::Idle) => {
                            idle_pending = true;
                        }
                        Err(e) => {
                            warn!(
                                vertex = self.ctx.vertex_id,
                                error = %e,
                                "fetch_next error"
                            );
                            return Err(RunError::Operator(e));
                        }
                    }
                }
            }
        }

        self.operator.on_close(&mut self.ctx).await?;

        info!(
            vertex = self.ctx.vertex_id,
            subtask = self.ctx.subtask_idx,
            "source subtask shutdown"
        );
        Ok(())
    }
}
