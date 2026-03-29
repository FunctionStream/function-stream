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
// 1. 网络桩 (Stub)：为后续 gRPC/TCP 扩展预留孔位
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
// 2. 物理发送端点 (Physical Sender Endpoint)
// ========================================================================

/// 统一的物理发送端点。
/// 算子无需知道目标是同机还是异机，只管调用 `send`。
#[derive(Clone)]
pub enum PhysicalSender {
    /// 本地线程间传输，携带内存船票，零开销
    Local(mpsc::Sender<TrackedEvent>),
    /// 跨机网络传输，需要序列化，并在发送后丢弃本地船票
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
// 3. 物理接收端点 (Physical Receiver Endpoint)
// ========================================================================

pub type BoxedEventStream = Pin<Box<dyn Stream<Item = TrackedEvent> + Send>>;
