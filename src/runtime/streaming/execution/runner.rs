use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::MessageOperator;
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::protocol::control::ControlCommand;
use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::stream_out::StreamOutput;
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use super::tracker::barrier_aligner::{AlignmentStatus, BarrierAligner};
use super::tracker::watermark_tracker::WatermarkTracker;
use crate::runtime::streaming::network::endpoint::BoxedEventStream;
use std::collections::VecDeque;
use std::pin::Pin;
use tokio::sync::mpsc::Receiver;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{debug, error, info, warn};
use crate::sql::common::{CheckpointBarrier, Watermark};

pub struct SubtaskRunner {
    operator: Box<dyn MessageOperator>,
    ctx: TaskContext,
    inboxes: Vec<BoxedEventStream>,
    control_rx: Receiver<ControlCommand>,
}

impl SubtaskRunner {
    pub fn new(
        operator: Box<dyn MessageOperator>,
        ctx: TaskContext,
        inboxes: Vec<BoxedEventStream>,
        control_rx: Receiver<ControlCommand>,
    ) -> Self {
        Self { operator, ctx, inboxes, control_rx }
    }

    pub async fn run(mut self) -> Result<(), RunError> {
        let input_count = self.inboxes.len();
        info!(
            job_id = %self.ctx.job_id,
            vertex = self.ctx.vertex_id,
            subtask = self.ctx.subtask_idx,
            inputs = input_count,
            operator = %self.operator.name(),
            "subtask starting"
        );

        self.operator.on_start(&mut self.ctx).await?;

        if input_count == 0 {
            return self.run_source_loop().await;
        }

        let mut stream_map: StreamMap<usize, Pin<Box<dyn tokio_stream::Stream<Item = TrackedEvent> + Send>>> = StreamMap::new();
        for (i, inbox) in self.inboxes.into_iter().enumerate() {
            stream_map.insert(i, inbox);
        }

        let mut wm_tracker = WatermarkTracker::new(input_count);
        let mut barrier_aligner = BarrierAligner::new(input_count);
        let mut eof_count = 0usize;
        let mut closed_on_full_eof = false;

        let tick_interval = self.operator.tick_interval();
        let mut tick_sleep: Option<Pin<Box<tokio::time::Sleep>>> =
            tick_interval.map(|d| Box::pin(tokio::time::sleep(d)));
        let mut tick_index: u64 = 0;

        'run: loop {
            tokio::select! {
                biased;

                cmd_opt = self.control_rx.recv() => {
                    match cmd_opt {
                        None => {
                            debug!(
                                vertex = self.ctx.vertex_id,
                                subtask = self.ctx.subtask_idx,
                                "control channel closed"
                            );
                            break 'run;
                        }
                        Some(cmd) => {
                            info!(
                                vertex = self.ctx.vertex_id,
                                subtask = self.ctx.subtask_idx,
                                ?cmd,
                                "control command"
                            );
                            if Self::handle_control_command(&mut self.operator, &mut self.ctx, cmd)
                                .await?
                            {
                                break 'run;
                            }
                        }
                    }
                }

                next_item = stream_map.next() => {
                    let Some((input_idx, event)) = next_item else {
                        break 'run;
                    };

                    if barrier_aligner.is_blocked(input_idx)
                        && !matches!(event.event, StreamEvent::Barrier(_))
                    {
                        barrier_aligner.buffer_event(input_idx, event);
                    } else {
                        let mut work = VecDeque::new();
                        work.push_back((input_idx, event));
                        let mut exit_run = false;
                        let mut dispatch = EventDispatchState {
                            operator: &mut self.operator,
                            ctx: &mut self.ctx,
                            work: &mut work,
                            wm_tracker: &mut wm_tracker,
                            barrier_aligner: &mut barrier_aligner,
                            eof_count: &mut eof_count,
                            closed_on_full_eof: &mut closed_on_full_eof,
                            input_count,
                        };
                        while let Some((idx, ev)) = dispatch.work.pop_front() {
                            if Self::dispatch_stream_event(&mut dispatch, idx, ev).await? {
                                exit_run = true;
                                break;
                            }
                        }
                        if exit_run {
                            break 'run;
                        }
                    }
                }

                _ = async {
                    match tick_sleep.as_mut() {
                        Some(s) => s.as_mut().await,
                        None => std::future::pending().await,
                    }
                }, if tick_interval.is_some() => {
                    let outs = self
                        .operator
                        .process_tick(tick_index, &mut self.ctx)
                        .await?;
                    tick_index = tick_index.wrapping_add(1);
                    Self::dispatch_stream_outputs(&mut self.ctx, outs).await?;
                    if let (Some(d), Some(s)) = (tick_interval, tick_sleep.as_mut()) {
                        s.as_mut()
                            .reset(tokio::time::Instant::now() + d);
                    }
                }
            }
        }

        if !closed_on_full_eof {
            let close_outs = self.operator.on_close(&mut self.ctx).await?;
            Self::dispatch_stream_outputs(&mut self.ctx, close_outs).await?;
        }

        info!(
            vertex = self.ctx.vertex_id,
            subtask = self.ctx.subtask_idx,
            "subtask shutdown"
        );
        Ok(())
    }

    async fn run_source_loop(mut self) -> Result<(), RunError> {
        while let Some(cmd) = self.control_rx.recv().await {
            if Self::handle_control_command(&mut self.operator, &mut self.ctx, cmd).await? {
                break;
            }
        }
        let close_outs = self.operator.on_close(&mut self.ctx).await?;
        Self::dispatch_stream_outputs(&mut self.ctx, close_outs).await?;
        if !self.ctx.outboxes.is_empty() {
            self.ctx.broadcast(StreamEvent::EndOfStream).await?;
        }
        info!(
            vertex = self.ctx.vertex_id,
            subtask = self.ctx.subtask_idx,
            "Source subtask finished"
        );
        Ok(())
    }

    async fn handle_control_command(
        operator: &mut Box<dyn MessageOperator>,
        ctx: &mut TaskContext,
        cmd: ControlCommand,
    ) -> Result<bool, RunError> {
        if let ControlCommand::TriggerCheckpoint { barrier } = &cmd {
            let barrier: CheckpointBarrier = barrier.clone().into();
            if let Err(e) = operator.snapshot_state(barrier, ctx).await {
                error!("Source snapshot failed: {}", e);
            }
            ctx.broadcast(StreamEvent::Barrier(barrier)).await?;
        }

        if let ControlCommand::Commit { epoch } = &cmd {
            if let Err(e) = operator.commit_checkpoint(*epoch, ctx).await {
                error!("commit_checkpoint failed: {}", e);
            }
        }

        match operator.handle_control(cmd, ctx).await {
            Ok(should_stop) => Ok(should_stop),
            Err(e) => {
                warn!("handle_control error: {}", e);
                Ok(false)
            }
        }
    }

    async fn dispatch_stream_outputs(
        ctx: &mut TaskContext,
        outputs: Vec<StreamOutput>,
    ) -> Result<(), RunError> {
        for out in outputs {
            match out {
                StreamOutput::Forward(b) => ctx.collect(b).await?,
                StreamOutput::Keyed(hash, b) => ctx.collect_keyed(hash, b).await?,
                StreamOutput::Broadcast(b) => ctx.collect(b).await?,
                StreamOutput::Watermark(wm) => {
                    ctx.broadcast(StreamEvent::Watermark(wm)).await?;
                }
            }
        }
        Ok(())
    }

    async fn dispatch_stream_event(
        st: &mut EventDispatchState<'_>,
        input_idx: usize,
        tracked: TrackedEvent,
    ) -> Result<bool, RunError> {
        let event = tracked.event;
        match event {
            StreamEvent::Data(batch) => {
                let outputs = st
                    .operator
                    .process_data(input_idx, batch, st.ctx)
                    .await?;
                Self::dispatch_stream_outputs(st.ctx, outputs).await?;
            }
            StreamEvent::Watermark(wm) => {
                if let Some(aligned_wm) = st.wm_tracker.update(input_idx, wm) {
                    if let Watermark::EventTime(t) = aligned_wm {
                        st.ctx.advance_watermark(t);
                    }
                    let outputs = st
                        .operator
                        .process_watermark(aligned_wm.clone(), st.ctx)
                        .await?;
                    Self::dispatch_stream_outputs(st.ctx, outputs).await?;
                    st.ctx
                        .broadcast(StreamEvent::Watermark(aligned_wm))
                        .await?;
                }
            }
            StreamEvent::Barrier(barrier) => {
                match st.barrier_aligner.mark(input_idx, &barrier) {
                    AlignmentStatus::Pending => {}
                    AlignmentStatus::Complete(buffered) => {
                        if let Err(e) = st.operator.snapshot_state(barrier, st.ctx).await {
                            error!("Operator snapshot failed: {}", e);
                        }
                        st.ctx.broadcast(StreamEvent::Barrier(barrier)).await?;
                        for pair in buffered {
                            st.work.push_back(pair);
                        }
                    }
                }
            }
            StreamEvent::EndOfStream => {
                *st.eof_count += 1;
                if *st.eof_count == st.input_count {
                    let close_outs = st.operator.on_close(st.ctx).await?;
                    Self::dispatch_stream_outputs(st.ctx, close_outs).await?;
                    *st.closed_on_full_eof = true;
                    st.ctx.broadcast(StreamEvent::EndOfStream).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

struct EventDispatchState<'a> {
    operator: &'a mut Box<dyn MessageOperator>,
    ctx: &'a mut TaskContext,
    work: &'a mut VecDeque<(usize, TrackedEvent)>,
    wm_tracker: &'a mut WatermarkTracker,
    barrier_aligner: &'a mut BarrierAligner,
    eof_count: &'a mut usize,
    closed_on_full_eof: &'a mut bool,
    input_count: usize,
}
