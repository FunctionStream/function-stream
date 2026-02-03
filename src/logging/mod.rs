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

use crate::config::{LogConfig, get_app_log_path, get_log_path, get_logs_dir};
use anyhow::Result;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging(config: &LogConfig) -> Result<()> {
    let (log_dir, log_file) = if let Some(ref file_path) = config.file_path {
        let path = PathBuf::from(file_path);
        if path.is_absolute() {
            let dir = path
                .parent()
                .unwrap_or_else(|| Path::new("logs"))
                .to_path_buf();
            (dir, path)
        } else {
            let full_path = get_log_path(file_path);
            let dir = full_path.parent().unwrap_or(&get_logs_dir()).to_path_buf();
            (dir, full_path)
        }
    } else {
        let file = get_app_log_path();
        (get_logs_dir(), file)
    };

    std::fs::create_dir_all(&log_dir)?;

    let log_level = config.level.parse::<EnvFilter>().unwrap_or_else(|_| {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    });

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(file);

    let subscriber = Registry::default()
        .with(log_level)
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .json(),
        )
        .with(fmt::layer().with_writer(std::io::stdout).with_ansi(true));

    subscriber.init();

    tracing::info!("Logging initialized, log file: {}", log_file.display());

    std::mem::forget(_guard);

    Ok(())
}
