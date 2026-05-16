//! Logging setup and helpers.

pub mod config {
    pub use function_stream_config::*;
}

mod logging;

pub use logging::init_logging;
