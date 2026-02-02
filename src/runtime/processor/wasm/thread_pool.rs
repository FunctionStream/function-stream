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

use crate::runtime::common::ComponentState;
use crate::runtime::processor::wasm::wasm_task::WasmTask;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct TaskThreadPool {
    tasks: Arc<Mutex<HashMap<String, TaskHandle>>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadGroupType {
    MainRunloop,
    InputSource(usize),
    OutputSink(usize),
    Cleanup,
}

pub struct ThreadInfo {
    handle: thread::JoinHandle<()>,
    is_running: Arc<AtomicBool>,
}

impl ThreadInfo {
    fn new(handle: thread::JoinHandle<()>) -> Self {
        Self {
            handle,
            is_running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    fn join(self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running.store(false, Ordering::Relaxed);
        self.handle
            .join()
            .map_err(|e| format!("Thread join error: {:?}", e))?;
        Ok(())
    }
}

pub struct ThreadGroup {
    pub group_type: ThreadGroupType,
    pub group_name: String,
    pub threads: Vec<ThreadInfo>,
    pub is_running: Arc<AtomicBool>,
}

impl ThreadGroup {
    pub fn new(group_type: ThreadGroupType, group_name: String) -> Self {
        Self {
            group_type,
            group_name,
            threads: Vec::new(),
            is_running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn add_thread(&mut self, handle: thread::JoinHandle<()>) {
        self.threads.push(ThreadInfo::new(handle));
    }

    pub fn is_finished(&self) -> bool {
        self.threads.is_empty() || self.threads.iter().all(|t| t.is_finished())
    }

    pub fn thread_count(&self) -> usize {
        self.threads.len()
    }

    pub fn running_thread_count(&self) -> usize {
        self.threads.iter().filter(|t| !t.is_finished()).count()
    }

    pub fn join_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running.store(false, Ordering::Relaxed);
        for thread_info in std::mem::take(&mut self.threads) {
            thread_info.join()?;
        }
        Ok(())
    }

    pub fn join_all_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        self.is_running.store(false, Ordering::Relaxed);

        let mut remaining_threads = std::mem::take(&mut self.threads);
        while !remaining_threads.is_empty() {
            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout waiting for {} threads in group '{}'",
                    remaining_threads.len(),
                    self.group_name
                )
                .into());
            }

            remaining_threads.retain(|t| if t.is_finished() { false } else { true });

            if !remaining_threads.is_empty() {
                thread::sleep(Duration::from_millis(10));
            }
        }

        Ok(())
    }
}

struct TaskHandle {
    task: Arc<Mutex<WasmTask>>,
    thread_groups: Vec<ThreadGroup>,
}

impl TaskHandle {
    fn new(task: Arc<Mutex<WasmTask>>) -> Self {
        Self {
            task,
            thread_groups: Vec::new(),
        }
    }

    fn add_thread_group(&mut self, thread_group: ThreadGroup) {
        self.thread_groups.push(thread_group);
    }

    fn get_all_thread_groups(&self) -> &[ThreadGroup] {
        &self.thread_groups
    }

    fn join_all_threads(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(main_thread) = self
            .thread_groups
            .iter_mut()
            .find(|g| matches!(g.group_type, ThreadGroupType::MainRunloop))
        {
            if let Some(timeout) = timeout {
                let _ = main_thread.join_all_with_timeout(timeout);
            } else {
                let _ = main_thread.join_all();
            }
        }

        for thread_group in &mut self.thread_groups {
            if let Some(timeout) = timeout {
                let _ = thread_group.join_all_with_timeout(timeout);
            } else {
                let _ = thread_group.join_all();
            }
        }

        Ok(())
    }
}

impl TaskThreadPool {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn submit(&self, task: Arc<Mutex<WasmTask>>) -> Result<String, Box<dyn std::error::Error>> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err("Thread pool is shutdown".into());
        }

        let task_id = {
            let task_guard = task.lock().unwrap();
            task_guard.get_name().to_string()
        };

        let tasks_clone = self.tasks.clone();
        let task_id_clone = task_id.clone();
        let shutdown_flag = self.shutdown.clone();

        let task_arc_for_cleanup = task.clone();
        let cleanup_thread = thread::Builder::new()
            .name(format!("TaskCleanup-{}", task_id_clone))
            .spawn(move || {
                Self::cleanup_task_thread(
                    task_arc_for_cleanup,
                    tasks_clone,
                    task_id_clone,
                    shutdown_flag,
                );
            })
            .map_err(|e| format!("Failed to spawn cleanup thread: {}", e))?;

        let mut cleanup_thread_group =
            ThreadGroup::new(ThreadGroupType::Cleanup, format!("TaskCleanup-{}", task_id));
        cleanup_thread_group.add_thread(cleanup_thread);

        let thread_groups = {
            let mut task_guard = task.lock().unwrap();
            task_guard.take_thread_groups()
        };

        let mut tasks = self.tasks.lock().unwrap();
        let mut handle = TaskHandle::new(task);

        if let Some(groups) = thread_groups {
            for group in groups {
                handle.add_thread_group(group);
            }
        }

        handle.add_thread_group(cleanup_thread_group);

        tasks.insert(task_id.clone(), handle);

        Ok(task_id)
    }

    fn cleanup_task_thread(
        task_arc: Arc<Mutex<WasmTask>>,
        tasks: Arc<Mutex<HashMap<String, TaskHandle>>>,
        task_id: String,
        shutdown_flag: Arc<AtomicBool>,
    ) {
        loop {
            let state = {
                let task_guard = task_arc.lock().unwrap();
                task_guard.get_state()
            };

            if matches!(state, ComponentState::Closed | ComponentState::Error { .. }) {
                break;
            }

            thread::sleep(Duration::from_millis(100));
        }

        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 50;

        loop {
            let task_guard = task_arc.lock().unwrap();
            let state = task_guard.get_state();
            drop(task_guard);

            if matches!(state, ComponentState::Closed) {
                break;
            }

            attempts += 1;
            if attempts >= MAX_ATTEMPTS {
                log::warn!("Timeout waiting for task {} to close", task_id);
                break;
            }

            thread::sleep(Duration::from_millis(100));
        }

        if !shutdown_flag.load(Ordering::Relaxed) {
            let mut tasks_guard = tasks.lock().unwrap();
            tasks_guard.remove(&task_id);
        }
    }

    pub fn get_task(&self, task_id: &str) -> Option<Arc<Mutex<WasmTask>>> {
        let tasks = self.tasks.lock().unwrap();
        tasks.get(task_id).map(|handle| handle.task.clone())
    }

    pub fn cancel_task(&self, task_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(task) = self.get_task(task_id) {
            let task_guard = task.lock().unwrap();
            task_guard.cancel().map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to cancel task: {}",
                    e
                ))) as Box<dyn std::error::Error>
            })?;
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    pub fn get_all_task_ids(&self) -> Vec<String> {
        let tasks = self.tasks.lock().unwrap();
        tasks.keys().cloned().collect()
    }

    pub fn task_count(&self) -> usize {
        let tasks = self.tasks.lock().unwrap();
        tasks.len()
    }

    pub fn wait_for_all_tasks(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        loop {
            let count = self.task_count();
            if count == 0 {
                return Ok(());
            }

            if let Some(timeout) = timeout
                && start.elapsed() > timeout
            {
                return Err(format!("Timeout waiting for {} tasks to complete", count).into());
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn cleanup_finished_threads(&self) -> usize {
        let mut cleaned_count = 0;
        let tasks = self.tasks.lock().unwrap();
        let task_ids: Vec<String> = tasks.keys().cloned().collect();
        drop(tasks);

        for task_id in task_ids {
            if let Some(task_arc) = self.get_task(&task_id) {
                let task_guard = task_arc.lock().unwrap();

                let state = task_guard.get_state();
                let is_finished =
                    matches!(state, ComponentState::Closed | ComponentState::Error { .. });

                if is_finished {
                    cleaned_count += 1;
                }
                drop(task_guard);
            }
        }

        cleaned_count
    }

    pub fn force_cleanup_all_threads(
        &self,
        timeout: Duration,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let mut cleaned_count = 0;
        let tasks = self.tasks.lock().unwrap();
        let task_ids: Vec<String> = tasks.keys().cloned().collect();
        drop(tasks);

        let start = std::time::Instant::now();
        for task_id in task_ids {
            if start.elapsed() > timeout {
                break;
            }

            if let Some(task_arc) = self.get_task(&task_id) {
                let task_guard = task_arc.lock().unwrap();
                let state = task_guard.get_state();
                if matches!(state, ComponentState::Closed | ComponentState::Error { .. }) {
                    cleaned_count += 1;
                }
                drop(task_guard);
            }
        }

        Ok(cleaned_count)
    }

    pub fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.shutdown.store(true, Ordering::Relaxed);

        let task_ids: Vec<String> = self.get_all_task_ids();
        for task_id in task_ids {
            let _ = self.cancel_task(&task_id);
        }

        self.wait_for_all_tasks(Some(Duration::from_secs(30)))?;

        let cleaned = self.cleanup_finished_threads();
        if cleaned > 0 {
            log::info!("Cleaned up {} finished threads", cleaned);
        }

        let remaining = self.task_count();
        if remaining > 0 {
            log::warn!("{} tasks still remain, forcing thread cleanup", remaining);
            let _ = self.force_cleanup_all_threads(Duration::from_secs(5));
        }

        self.wait_for_cleanup_threads(Duration::from_secs(10))?;

        let final_count = self.task_count();
        if final_count > 0 {
            return Err(format!("{} tasks still remain after shutdown", final_count).into());
        }

        Ok(())
    }

    fn wait_for_cleanup_threads(
        &self,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        loop {
            let tasks = self.tasks.lock().unwrap();
            let all_cleanup_done = tasks.values().all(|handle| {
                handle
                    .thread_groups
                    .iter()
                    .find(|g| matches!(g.group_type, ThreadGroupType::Cleanup))
                    .map(|g| g.is_finished())
                    .unwrap_or(true)
            });
            drop(tasks);

            if all_cleanup_done {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err("Timeout waiting for cleanup threads".into());
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn shutdown_now(&self) {
        self.shutdown.store(true, Ordering::Relaxed);

        let task_ids: Vec<String> = self.get_all_task_ids();
        for task_id in task_ids {
            let _ = self.cancel_task(&task_id);
        }

        let _ = self.cleanup_finished_threads();
    }

    pub fn get_thread_health_status(&self) -> ThreadHealthStatus {
        let tasks = self.tasks.lock().unwrap();
        let mut total_tasks = 0;
        let mut alive_threads = 0;
        let mut finished_threads = 0;
        let mut zombie_threads = 0;

        for handle in tasks.values() {
            total_tasks += 1;

            let mut task_alive_threads = 0;
            let mut task_finished_threads = 0;

            for thread_group in &handle.thread_groups {
                let running = thread_group.running_thread_count();
                let finished = thread_group.thread_count() - running;
                task_alive_threads += running;
                task_finished_threads += finished;
            }

            if let Ok(task_guard) = handle.task.try_lock() {
                let state = task_guard.get_state();
                let is_finished =
                    matches!(state, ComponentState::Closed | ComponentState::Error { .. });

                if task_alive_threads > 0 {
                    alive_threads += task_alive_threads;
                    if is_finished {
                        zombie_threads += task_alive_threads;
                    }
                }
                if task_finished_threads > 0 {
                    finished_threads += task_finished_threads;
                }
            }
        }

        ThreadHealthStatus {
            total_tasks,
            alive_threads,
            finished_threads,
            zombie_threads,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub struct ThreadHealthStatus {
    pub total_tasks: usize,
    pub alive_threads: usize,
    pub finished_threads: usize,
    pub zombie_threads: usize,
}

impl ThreadHealthStatus {
    pub fn has_zombie_threads(&self) -> bool {
        self.zombie_threads > 0
    }

    pub fn all_threads_recycled(&self) -> bool {
        self.total_tasks == 0
            || (self.alive_threads == 0 && self.finished_threads == self.total_tasks)
    }
}

impl Default for TaskThreadPool {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GlobalTaskThreadPool;

impl GlobalTaskThreadPool {
    pub fn get_or_create() -> Arc<TaskThreadPool> {
        static POOL: std::sync::OnceLock<Arc<TaskThreadPool>> = std::sync::OnceLock::new();
        POOL.get_or_init(|| Arc::new(TaskThreadPool::default()))
            .clone()
    }
}
