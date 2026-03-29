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

use crate::runtime::streaming::protocol::event::StreamEvent;
use crate::runtime::streaming::protocol::tracked::TrackedEvent;
use anyhow::{anyhow, Result};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tracing::debug;

// ========================================================================
// ========================================================================

#[derive(Clone)]
pub struct RemoteSenderStub {
    pub target_addr: String,
}

impl RemoteSenderStub {
    pub async fn send_over_network(&self, _event: &StreamEvent) -> Result<()> {
        unimplemented!("Remote network transport is not yet implemented")
    }
}

// ========================================================================
// ========================================================================

#[derive(Clone)]
pub enum PhysicalSender {
    Local(mpsc::Sender<TrackedEvent>),
    Remote(RemoteSenderStub),
}

impl PhysicalSender {
    pub async fn send(&self, tracked_event: TrackedEvent) -> Result<()> {
        match self {
            PhysicalSender::Local(tx) => {
                tx.send(tracked_event)
                    .await
                    .map_err(|_| anyhow!("Local channel closed! Downstream task may have crashed."))?;
            }
            PhysicalSender::Remote(stub) => {
                stub.send_over_network(&tracked_event.event).await?;
                debug!("Sent event over network, local memory ticket will be released.");
            }
        }
        Ok(())
    }
}

// ========================================================================
// ========================================================================

pub type BoxedEventStream = Pin<Box<dyn Stream<Item = TrackedEvent> + Send>>;
