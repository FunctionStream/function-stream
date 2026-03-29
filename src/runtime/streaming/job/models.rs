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
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use protocol::grpc::api::FsProgram;
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
