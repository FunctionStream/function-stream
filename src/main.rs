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

#![allow(dead_code)]

mod api;
mod config;
mod coordinator;
mod logging;
mod runtime;
mod server;
mod sql;
mod storage;
mod types;

use anyhow::{Context, Result};
use std::thread;
use tokio::sync::oneshot;

pub struct ServerHandle {
    join_handle: Option<thread::JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    error_rx: oneshot::Receiver<anyhow::Error>,
}

impl ServerHandle {
    pub fn stop(mut self) {
        log::info!("Initiating server shutdown sequence...");

        if let Some(tx) = self.shutdown_tx.take()
            && tx.send(()).is_err()
        {
            log::warn!("Server shutdown signal failed to send (receiver dropped)");
        }

        if let Some(handle) = self.join_handle.take() {
            log::info!("Waiting for server thread to finalize...");
            if let Err(e) = handle.join() {
                log::error!("Failed to join server thread: {:?}", e);
            }
        }

        log::info!("Server shutdown completed.");
    }

    pub async fn wait_for_error(&mut self) -> Result<()> {
        if let Ok(err) = (&mut self.error_rx).await {
            return Err(err);
        }
        Ok(())
    }
}

async fn wait_for_signal() -> Result<String> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).context("Failed to register SIGTERM")?;
        let mut sigint = signal(SignalKind::interrupt()).context("Failed to register SIGINT")?;
        let mut sighup = signal(SignalKind::hangup()).context("Failed to register SIGHUP")?;

        tokio::select! {
            _ = sigterm.recv() => Ok("SIGTERM".to_string()),
            _ = sigint.recv() => Ok("SIGINT".to_string()),
            _ = sighup.recv() => Ok("SIGHUP".to_string()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("Failed to listen for Ctrl+C")?;
        Ok("Ctrl+C".to_string())
    }
}

fn spawn_server_thread(config: config::GlobalConfig) -> Result<ServerHandle> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (error_tx, error_rx) = oneshot::channel();

    let cpu_count = num_cpus::get();
    let worker_threads = config.service.workers.unwrap_or_else(|| {
        let multiplier = config.service.worker_multiplier.unwrap_or(4);
        cpu_count * multiplier
    });

    log::info!(
        "Spawning gRPC server thread (Workers: {}, Cores: {})",
        worker_threads,
        cpu_count
    );

    let handle = thread::Builder::new()
        .name("grpc-runtime".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .thread_name("grpc-worker")
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = error_tx.send(anyhow::anyhow!("Failed to build runtime: {}", e));
                    return;
                }
            };

            rt.block_on(async {
                if let Err(e) = server::start_server_with_shutdown(&config, shutdown_rx, None).await
                {
                    log::error!("Server runtime loop crashed: {}", e);
                    let _ = error_tx.send(e);
                }
            });
        })
        .context("Failed to spawn server thread")?;

    Ok(ServerHandle {
        join_handle: Some(handle),
        shutdown_tx: Some(shutdown_tx),
        error_rx,
    })
}

fn setup_environment() -> Result<config::GlobalConfig> {
    let data_dir = config::get_data_dir();
    let conf_dir = config::get_conf_dir();

    let config = if let Some(path) = config::find_config_file("config.yaml") {
        log::info!("Loading configuration from: {}", path.display());
        config::load_global_config(&path)
            .map_err(|e| anyhow::anyhow!("{}", e))
            .context("Configuration load failed")?
    } else {
        log::warn!("Configuration file not found, defaulting to built-in values.");
        config::GlobalConfig::default()
    };

    logging::init_logging(&config.logging).context("Logging initialization failed")?;

    log::debug!(
        "Environment initialized. Data: {}, Conf: {}",
        data_dir.display(),
        conf_dir.display()
    );
    Ok(config)
}

fn main() -> Result<()> {
    // 1. Bootstrap
    let config = match setup_environment() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Bootstrap failure: {:#}", e);
            std::process::exit(1);
        }
    };

    config
        .validate()
        .map_err(|e| anyhow::anyhow!(e))
        .context("Configuration validation failed")?;

    proctitle::set_title(format!("function-stream-{}", config.service.service_id));
    log::info!(
        "Starting Service [Name: {}, ID: {}] on {}:{}",
        config.service.service_name,
        config.service.service_id,
        config.service.host,
        config.service.port
    );

    // 2. Component Initialization
    let registry = server::register_components();
    registry
        .initialize_all(&config)
        .context("Component initialization failed")?;

    // 3. Server Startup
    let mut server_handle = spawn_server_thread(config.clone())?;
    log::info!("Service is running and accepting requests.");

    // 4. Main Event Loop
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Control plane runtime failed")?;

    let exit_result = rt.block_on(async {
        tokio::select! {
            // Case A: Server crashed internally
            err = server_handle.wait_for_error() => {
                log::error!("Server process exited unexpectedly.");
                Err(err.unwrap_err())
            }
            // Case B: System signal received
            sig = wait_for_signal() => {
                log::info!("Received signal: {}. shutting down...", sig.unwrap_or_default());
                Ok(())
            }
        }
    });

    // 5. Teardown
    match exit_result {
        Ok(_) => {
            server_handle.stop();
            log::info!("Service stopped gracefully.");
            Ok(())
        }
        Err(e) => {
            log::error!("Service terminated with error: {:#}", e);
            std::process::exit(1);
        }
    }
}
