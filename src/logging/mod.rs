// Logging module - simple file logging to logs directory

use crate::config::LogConfig;
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};

/// Initialize logging with file output to logs directory
pub fn init_logging(config: &LogConfig) -> Result<()> {
    // Find or create logs directory using the same path resolution as data/conf
    let base_logs_dir = crate::config::find_or_create_logs_dir()
        .map_err(|e| anyhow::anyhow!("Failed to find or create logs directory: {}", e))?;
    
    // Determine log directory and file path
    let (log_dir, log_file) = if let Some(ref file_path) = config.file_path {
        let path = PathBuf::from(file_path);
        // If path is relative, resolve it relative to base_logs_dir
        // If path is absolute, use it as-is
        if path.is_absolute() {
        let dir = path.parent().unwrap_or_else(|| Path::new("logs")).to_path_buf();
        (dir, path)
        } else {
            // Relative path: resolve relative to base_logs_dir
            let full_path = base_logs_dir.join(&path);
            let dir = full_path.parent().unwrap_or(&base_logs_dir).to_path_buf();
            (dir, full_path)
        }
    } else {
        let file = base_logs_dir.join("app.log");
        (base_logs_dir, file)
    };

    // Create log directory if it doesn't exist (should already exist, but ensure it)
    std::fs::create_dir_all(&log_dir)?;

    // Parse log level
    let log_level = config.level.parse::<EnvFilter>().unwrap_or_else(|_| {
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
    });

    // Open log file for appending
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(file);

    // Build subscriber with file and stdout output
    let subscriber = Registry::default()
        .with(log_level)
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .json(),
        )
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_ansi(true),
        );

    // Initialize
    subscriber.init();

    tracing::info!("Logging initialized, log file: {}", log_file.display());

    // Keep the guard alive (prevents log file from being closed)
    std::mem::forget(_guard);

    Ok(())
}
