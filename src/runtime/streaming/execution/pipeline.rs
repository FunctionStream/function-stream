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
use tokio_stream::{StreamExt, StreamMap};
use tracing::{Instrument, info, info_span};

use crate::runtime::streaming::api::context::TaskContext;
use crate::runtime::streaming::api::operator::Operator;
use crate::runtime::streaming::error::RunError;
use crate::runtime::streaming::execution::operator_chain::{ChainBuilder, OperatorDrive};
use crate::runtime::streaming::execution::tracker::{
    barrier_aligner::{AlignmentStatus, BarrierAligner},
    watermark_tracker::WatermarkTracker,
};
use crate::runtime::streaming::network::endpoint::BoxedEventStream;
use crate::runtime::streaming::protocol::{
    control::ControlCommand,
    event::{StreamEvent, TrackedEvent},
};
use crate::sql::common::Watermark;

pub struct Pipeline {
    chain_head: Box<dyn OperatorDrive>,
    ctx: TaskContext,
    inboxes: Vec<BoxedEventStream>,
    control_rx: Receiver<ControlCommand>,

    wm_tracker: WatermarkTracker,
    barrier_aligner: BarrierAligner,
    paused_streams: Vec<Option<BoxedEventStream>>,
}

impl Pipeline {
    pub fn new(
        operators: Vec<Box<dyn Operator>>,
        ctx: TaskContext,
        inboxes: Vec<BoxedEventStream>,
        control_rx: Receiver<ControlCommand>,
    ) -> Result<Self, RunError> {
        let input_count = inboxes.len();
        let chain_head = ChainBuilder::build(operators)
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
