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

// gRPC server setup and management

use crate::config::GlobalConfig;
use crate::server::FunctionStreamServiceImpl;
use anyhow::Result;
use protocol::service::function_stream_service_server::FunctionStreamServiceServer;
use std::net::SocketAddr;
use tonic::transport::Server;

/// Start the gRPC server with shutdown signal
pub async fn start_server_with_shutdown(
    config: &GlobalConfig,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", config.service.host, config.service.port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid address format: {}", e))?;

    log::info!("Starting gRPC server on {}", addr);

    // Create service implementation
    let service_impl = FunctionStreamServiceImpl::new();

    // Create gRPC server with shutdown signal
    let server = Server::builder()
        .add_service(FunctionStreamServiceServer::new(service_impl))
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
            log::info!("Shutdown signal received, stopping gRPC server...");
        });

    // Spawn server task and wait for it to bind to address
    let server_handle = tokio::spawn(server);

    // Wait a bit for server to bind, then notify ready
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Notify that server is ready (after binding to address)
    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    // Wait for server to run (this will block until server stops or errors)
    server_handle
        .await
        .map_err(|e| anyhow::anyhow!("Server task error: {}", e))?
        .map_err(|e| {
            let error_msg = format!("{}", e);
            // Check if it's a port binding error
            if error_msg.contains("address already in use") 
                || error_msg.contains("transport error")
                || error_msg.contains("bind")
                || error_msg.contains("EADDRINUSE") {
                anyhow::anyhow!("Port {} is already in use. Please stop the existing server or use a different port. Error: {}", addr.port(), e)
            } else {
                anyhow::anyhow!("Server runtime error: {}", e)
            }
        })?;

    log::info!("gRPC server stopped");

    Ok(())
}
