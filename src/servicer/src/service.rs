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

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::config::GlobalConfig;
use crate::coordinator::Coordinator;
use crate::server::FunctionStreamServiceImpl;
use protocol::service::function_stream_service_server::FunctionStreamServiceServer;

pub async fn start_server_with_shutdown(
    config: &GlobalConfig,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
    let addr_str = format!("{}:{}", config.service.host, config.service.port);
    let addr: SocketAddr = addr_str
        .parse()
        .with_context(|| format!("Invalid address format: {}", addr_str))?;

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind to address: {}", addr))?;

    log::info!("gRPC server listening on {}", addr);

    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    let coordinator = Arc::new(Coordinator::new());
    let service_impl = FunctionStreamServiceImpl::new(coordinator);

    let incoming = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(FunctionStreamServiceServer::new(service_impl))
        .serve_with_incoming_shutdown(incoming, async {
            shutdown_rx.await.ok();
            log::info!("Shutdown signal received, stopping gRPC server...");
        })
        .await
        .with_context(|| "gRPC server runtime error")?;

    log::info!("gRPC server stopped");

    Ok(())
}
