// TaskCompletionFlag - Task completion flag
//
// Used to track whether control tasks have completed processing, supports blocking wait and error message recording

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Condvar};
use std::time::Duration;

/// Default timeout (milliseconds)
pub const DEFAULT_COMPLETION_TIMEOUT_MS: u64 = 1000;

/// Task completion result
#[derive(Debug, Clone)]
pub enum TaskResult {
    /// Task completed successfully
    Success,
    /// Task failed
    Error(String),
}

impl TaskResult {
    /// Check if successful
    pub fn is_success(&self) -> bool {
        matches!(self, TaskResult::Success)
    }

    /// Check if failed
    pub fn is_error(&self) -> bool {
        matches!(self, TaskResult::Error(_))
    }

    /// Get error message
    pub fn error_message(&self) -> Option<&str> {
        match self {
            TaskResult::Error(msg) => Some(msg),
            TaskResult::Success => None,
        }
    }
}

/// Task completion flag
/// 
/// Used to track whether control tasks have completed processing
/// 
/// Supports:
/// - Blocking wait (using Condvar, default timeout 1 second)
/// - Non-blocking check
/// - Completion notification (wake up all waiting threads)
/// - Error message recording
#[derive(Debug, Clone)]
pub struct TaskCompletionFlag {
    /// Completion flag
    completed: Arc<AtomicBool>,
    /// Condition variable (for blocking wait notification)
    condvar: Arc<(Mutex<bool>, Condvar)>,
    /// Task result (success or error message)
    result: Arc<Mutex<Option<TaskResult>>>,
}

impl TaskCompletionFlag {
    /// Create new task completion flag
    pub fn new() -> Self {
        Self {
            completed: Arc::new(AtomicBool::new(false)),
            condvar: Arc::new((Mutex::new(false), Condvar::new())),
            result: Arc::new(Mutex::new(None)),
        }
    }

    /// Mark task as successfully completed and notify all waiting threads
    pub fn mark_completed(&self) {
        self.complete_with_result(TaskResult::Success);
    }

    /// Mark task as failed and notify all waiting threads
    /// 
    /// # Arguments
    /// - `error`: Error message
    pub fn mark_error(&self, error: String) {
        self.complete_with_result(TaskResult::Error(error));
    }

    /// Complete task with specified result
    fn complete_with_result(&self, task_result: TaskResult) {
        // Save result
        {
            let mut result = self.result.lock().unwrap();
            *result = Some(task_result);
        }

        // Set completion flag
        self.completed.store(true, Ordering::SeqCst);

        // Notify all waiting threads
        let (lock, cvar) = &*self.condvar;
        let mut completed = lock.lock().unwrap();
        *completed = true;
        cvar.notify_all();
    }

    /// Check if task is completed (non-blocking)
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Check if task completed successfully
    pub fn is_success(&self) -> bool {
        if let Ok(result) = self.result.lock() {
            result.as_ref().map(|r| r.is_success()).unwrap_or(false)
        } else {
            false
        }
    }

    /// Check if task failed
    pub fn is_error(&self) -> bool {
        if let Ok(result) = self.result.lock() {
            result.as_ref().map(|r| r.is_error()).unwrap_or(false)
        } else {
            false
        }
    }

    /// Get task result
    pub fn get_result(&self) -> Option<TaskResult> {
        self.result.lock().ok().and_then(|r| r.clone())
    }

    /// Get error message
    pub fn get_error(&self) -> Option<String> {
        self.result.lock().ok().and_then(|r| {
            r.as_ref().and_then(|res| res.error_message().map(|s| s.to_string()))
        })
    }

    /// Blocking wait for task completion (default timeout 1 second)
    /// 
    /// # Returns
    /// - `Ok(())`: Task completed successfully
    /// - `Err(String)`: Task failed or timeout
    pub fn wait(&self) -> Result<(), String> {
        self.wait_timeout(Duration::from_millis(DEFAULT_COMPLETION_TIMEOUT_MS))
    }

    /// Blocking wait for task completion (specified timeout)
    /// 
    /// # Arguments
    /// - `timeout`: Timeout duration
    /// 
    /// # Returns
    /// - `Ok(())`: Task completed successfully
    /// - `Err(String)`: Task failed or timeout
    pub fn wait_timeout(&self, timeout: Duration) -> Result<(), String> {
        // Quick check
        if self.is_completed() {
            return self.check_result();
        }

        let (lock, cvar) = &*self.condvar;
        let completed = lock.lock().unwrap();
        
        if *completed {
            return self.check_result();
        }

        // Use condition variable to block wait for notification
        let result = cvar.wait_timeout(completed, timeout).unwrap();
        
        if *result.0 || self.is_completed() {
            self.check_result()
        } else {
            Err("Task completion timeout".to_string())
        }
    }

    /// Check task result
    fn check_result(&self) -> Result<(), String> {
        match self.get_result() {
            Some(TaskResult::Success) => Ok(()),
            Some(TaskResult::Error(e)) => Err(e),
            None => Err("Task result not set".to_string()),
        }
    }

    /// Blocking wait for task completion (wait forever)
    /// 
    /// # Returns
    /// - `Ok(())`: Task completed successfully
    /// - `Err(String)`: Task failed
    pub fn wait_forever(&self) -> Result<(), String> {
        if self.is_completed() {
            return self.check_result();
        }

        let (lock, cvar) = &*self.condvar;
        let mut completed = lock.lock().unwrap();
        
        while !*completed && !self.is_completed() {
            completed = cvar.wait(completed).unwrap();
        }

        self.check_result()
    }
}

impl Default for TaskCompletionFlag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_task_completion_success() {
        let flag = TaskCompletionFlag::new();
        
        assert!(!flag.is_completed());
        assert!(!flag.is_success());
        
        flag.mark_completed();
        
        assert!(flag.is_completed());
        assert!(flag.is_success());
        assert!(!flag.is_error());
        assert!(flag.wait().is_ok());
    }

    #[test]
    fn test_task_completion_error() {
        let flag = TaskCompletionFlag::new();
        
        flag.mark_error("Test error".to_string());
        
        assert!(flag.is_completed());
        assert!(flag.is_error());
        assert!(!flag.is_success());
        assert_eq!(flag.get_error(), Some("Test error".to_string()));
        assert!(flag.wait().is_err());
    }

    #[test]
    fn test_task_completion_wait_timeout() {
        let flag = TaskCompletionFlag::new();
        
        let result = flag.wait_timeout(Duration::from_millis(10));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Task completion timeout");
    }

    #[test]
    fn test_task_completion_cross_thread() {
        let flag = TaskCompletionFlag::new();
        let flag_clone = flag.clone();
        
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            flag_clone.mark_completed();
        });
        
        let result = flag.wait_timeout(Duration::from_secs(1));
        assert!(result.is_ok());
        
        handle.join().unwrap();
    }
}

