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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use protocol::function_stream_graph::FsProgram;
use tokio::sync::mpsc;

use crate::runtime::streaming::protocol::control::ControlCommand;

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStatus {
    Initializing,
    Running,
    Failed { error: String, is_panic: bool },
    Finished,
    Stopping,
}

/// Aggregated lifecycle / health label for an entire streaming job, computed from all
/// [`PhysicalPipeline`] [`PipelineStatus`] values.
///
/// This is a **roll-up**, not a per-pipeline state: it answers “how is the job as a whole
/// doing?” for listing, SQL result sets, and similar surfaces. Wire representation is a short
/// uppercase token (stable for clients).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamingJobRollupStatus {
    /// At least one pipeline has failed (error exit or panic). Other pipelines may still be
    /// running or stopping; the job requires operator attention.
    Degraded,
    /// Every pipeline is in [`PipelineStatus::Running`]: steady-state processing.
    Running,
    /// Every pipeline has reached [`PipelineStatus::Finished`]: the job graph has quiesced
    /// successfully (e.g. bounded job or graceful end-of-stream).
    Finished,
    /// No pipeline has failed, at least one is still [`PipelineStatus::Initializing`], and the
    /// job is not yet uniformly running—startup is still in progress.
    Initializing,
    /// No failures, but pipelines are in a **mixed** non-failed combination (e.g. some running
    /// and some stopping, or counts that do not match the all-running / all-finished rules).
    /// Often transient during control operations or uneven pipeline progress.
    Reconciling,
}

impl StreamingJobRollupStatus {
    /// Stable token exposed in APIs and SQL output (historical uppercase spelling).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Degraded => "DEGRADED",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::Initializing => "INITIALIZING",
            Self::Reconciling => "RECONCILING",
        }
    }
}

impl fmt::Display for StreamingJobRollupStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct PhysicalPipeline {
    pub pipeline_id: u32,
    pub handle: Option<JoinHandle<()>>,
    pub status: Arc<RwLock<PipelineStatus>>,
    pub control_tx: mpsc::Sender<ControlCommand>,
}

pub struct PhysicalExecutionGraph {
    pub job_id: String,
    pub program: FsProgram,
    pub pipelines: HashMap<u32, PhysicalPipeline>,
    pub start_time: Instant,
}
