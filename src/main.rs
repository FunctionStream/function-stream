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

mod codec;
mod config;
mod logging;
mod runtime;
mod server;
mod sql;
mod storage;

use anyhow::{Context, Result};
use tracing::info;

/// Wait for shutdown signal and return the signal type
/// Supports SIGTERM, SIGINT, SIGHUP on Unix, and Ctrl+C on all platforms
/// This function must be called from within a tokio runtime context
async fn wait_for_shutdown_signal() -> anyhow::Result<String> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).context("Failed to create SIGTERM signal handler")?;
        let mut sigint =
            signal(SignalKind::interrupt()).context("Failed to create SIGINT signal handler")?;
        let mut sighup =
            signal(SignalKind::hangup()).context("Failed to create SIGHUP signal handler")?;

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM signal, initiating graceful shutdown...");
                Ok("SIGTERM".to_string())
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT signal (Ctrl+C), initiating graceful shutdown...");
                Ok("SIGINT".to_string())
            }
            _ = sighup.recv() => {
                tracing::info!("Received SIGHUP signal, initiating graceful shutdown...");
                Ok("SIGHUP".to_string())
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("Failed to wait for Ctrl+C signal")?;
        tracing::info!("Received Ctrl+C signal, initiating graceful shutdown...");
        Ok("Ctrl+C".to_string())
    }
}

/// 初始化所有核心组件
///
/// 包括：
/// - TaskManager（任务管理器）
/// - 其他需要初始化的组件
///
/// # 参数
/// - `config`: 全局配置
///
/// # 返回值
/// - `Ok(())`: 初始化成功
/// - `Err(...)`: 初始化失败
fn initialize_components(config: &config::GlobalConfig) -> Result<()> {
    info!("Initializing core components...");

    // 初始化 TaskManager
    info!("Initializing TaskManager...");
    runtime::taskexecutor::TaskManager::init(config).context("Failed to initialize TaskManager")?;
    info!("TaskManager initialized successfully");

    // 初始化 Python WASM Runtime
    info!("Initializing Python WASM Runtime...");
    runtime::processor::Python::PythonService::initialize(config)
        .context("Failed to initialize Python WASM Runtime")?;
    info!("Python WASM Runtime initialized successfully");

    // TODO: 初始化其他组件
    // - Metrics registry
    // - Resource manager
    // - 等等

    info!("All core components initialized successfully");
    Ok(())
}

/// 启动 gRPC 服务器
///
/// 在后台线程中启动 gRPC 服务器，并返回服务器句柄和通信通道。
///
/// # 参数
/// - `config`: 全局配置
///
/// # 返回值
/// - `Ok((server_handle, shutdown_tx, error_rx))`: 成功启动
///   - `server_handle`: 服务器线程句柄
///   - `shutdown_tx`: 关闭信号发送通道
///   - `error_rx`: 错误接收通道
/// - `Err(...)`: 启动失败
fn start_server(
    config: &config::GlobalConfig,
) -> Result<(
    std::thread::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<anyhow::Error>,
)> {
    // 计算 RPC 线程数：CPU 核心数 × 倍数，或使用配置的 workers 值
    // 默认倍数为 4（如果配置文件中未指定）
    let cpu_count = num_cpus::get();
    let default_multiplier = 4;
    let (rpc_threads, calculation_info) = if let Some(workers) = config.service.workers {
        (workers, format!("configured value: {}", workers))
    } else {
        let multiplier = config
            .service
            .worker_multiplier
            .unwrap_or(default_multiplier);
        let threads = cpu_count * multiplier;
        (
            threads,
            format!(
                "CPU cores ({}) × multiplier ({}) = {}",
                cpu_count, multiplier, threads
            ),
        )
    };

    info!(
        "Starting gRPC server in background thread with {} RPC threads ({})...",
        rpc_threads, calculation_info
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (error_tx, error_rx) = tokio::sync::oneshot::channel();

    let config = config.clone();
    let thread_count = rpc_threads;
    let server_handle = std::thread::spawn(move || {
        // 创建 tokio runtime，使用配置的线程数
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count)
            .thread_name("grpc-worker")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for server");

        rt.block_on(async {
            if let Err(e) = server::start_server_with_shutdown(&config, shutdown_rx, None).await {
                tracing::error!("Server runtime error: {}", e);
                // 通知主线程服务器失败
                let _ = error_tx.send(e);
            }
        });
    });

    Ok((server_handle, shutdown_tx, error_rx))
}

/// Perform graceful shutdown of the service
fn graceful_shutdown(
    server_handle: std::thread::JoinHandle<()>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
) {
    tracing::info!("Shutting down service...");

    // Step 1: Send shutdown signal to server
    tracing::info!("Sending shutdown signal to gRPC server...");
    if shutdown_tx.send(()).is_err() {
        tracing::warn!("Failed to send shutdown signal, server may have already stopped");
    }

    // Step 2: Wait for server thread to finish
    tracing::info!("Waiting for gRPC server to stop...");
    if let Err(e) = server_handle.join() {
        tracing::error!("Error joining server thread: {:?}", e);
    }

    // Step 3: Wait for ongoing requests to complete
    tracing::info!("Waiting for ongoing requests to complete...");
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Step 4: Final cleanup
    tracing::info!("Performing final cleanup...");
    // TODO: Add cleanup for other resources (runtime manager, metrics, etc.)

    tracing::info!("Service shutdown complete. Goodbye!");
}

#[allow(clippy::too_many_lines)]
fn main() -> anyhow::Result<()> {
    // Find and ensure data and conf directories exist
    let data_dir = config::find_or_create_data_dir()
        .map_err(|e| anyhow::anyhow!("Failed to find or create data directory: {}", e))?;
    let conf_dir = config::find_or_create_conf_dir()
        .map_err(|e| anyhow::anyhow!("Failed to find or create conf directory: {}", e))?;

    tracing::debug!("Using data directory: {}", data_dir.display());
    tracing::debug!("Using conf directory: {}", conf_dir.display());

    // Find and load configuration file from multiple locations
    let config = if let Some(config_path) = config::find_config_file("config.yaml") {
        tracing::debug!("Loading configuration from: {}", config_path.display());
        config::load_global_config(&config_path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to load config file '{}': {}",
                config_path.display(),
                e
            )
        })?
    } else {
        tracing::info!("No config file found, using default configuration");
        config::GlobalConfig::default()
    };

    // Initialize logging to file
    logging::init_logging(&config.logging)
        .map_err(|e| anyhow::anyhow!("Failed to initialize logging: {}", e))?;

    info!("Starting function-stream service...");

    // Validate configuration
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Configuration validation failed: {}", e))?;

    info!(
        "Service configuration: {} (ID: {}) - {}:{}",
        config.service.service_name,
        config.service.service_id,
        config.service.host,
        config.service.port
    );
    info!("Log level: {}", config.logging.level);

    // Initialize core components (must be done before starting server)
    initialize_components(&config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize components: {}", e))?;

    // Start gRPC server
    let (server_handle, shutdown_tx, error_rx) =
        start_server(&config).map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

    info!("Service started successfully, waiting for requests...");

    // Wait for either shutdown signal or server error
    // Create a tokio runtime for signal handling (main thread doesn't have one)
    let rt = tokio::runtime::Runtime::new()
        .context("Failed to create tokio runtime for signal handling")?;

    let result = rt.block_on(async {
        tokio::select! {
            // Server error occurred
            server_error = error_rx => {
                if let Ok(err) = server_error {
                    Err(anyhow::anyhow!("Server failed: {}", err))
                } else {
                    // Channel closed, server exited normally
                    Ok(())
                }
            }
            // Shutdown signal received
            signal_result = wait_for_shutdown_signal() => {
                signal_result?;
                Ok(())
            }
        }
    });

    match result {
        Ok(_) => {
            let signal_type = "shutdown signal";
            tracing::debug!("Shutdown triggered by: {}", signal_type);
            // Perform graceful shutdown
            graceful_shutdown(server_handle, shutdown_tx);
        }
        Err(e) => {
            eprintln!("ERROR: {}", e);
            tracing::error!("Server failed, exiting: {}", e);
            // If server failed, don't wait for graceful shutdown, just exit
            // The server thread may have already exited
            let _ = server_handle.join();
            return Err(e);
        }
    }

    Ok(())
}
