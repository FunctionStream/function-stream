// AvailabilityProvider - Availability provider
//
// Used for asynchronous data availability checking

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// AvailabilityProvider - Availability provider interface
///
/// Used for asynchronous data availability checking
pub trait AvailabilityProvider: Send + Sync {
    /// Check if immediately available
    fn is_available(&self) -> bool;

    /// Get availability Future
    ///
    /// The Future completes when data becomes available
    fn get_available_future(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Simple availability provider implementation
pub struct SimpleAvailabilityProvider {
    available: Arc<std::sync::atomic::AtomicBool>,
}

impl SimpleAvailabilityProvider {
    pub fn new() -> Self {
        Self {
            available: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn set_available(&self, available: bool) {
        self.available
            .store(available, std::sync::atomic::Ordering::Relaxed);
    }
}

impl AvailabilityProvider for SimpleAvailabilityProvider {
    fn is_available(&self) -> bool {
        self.available.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn get_available_future(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            // Simple implementation, should use condition variables or channels in practice
            while !self.is_available() {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
    }
}

impl Default for SimpleAvailabilityProvider {
    fn default() -> Self {
        Self::new()
    }
}
