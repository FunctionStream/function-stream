// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Streaming actor runtime (vendored from Arroyo `arroyo-actor-runtime`).

pub mod api;
pub mod arrow;
pub mod cluster;
pub mod connectors;
pub mod error;
pub mod execution;
pub mod factory;
pub mod format;
pub mod memory;
pub mod network;
pub mod operators;
pub mod protocol;
pub mod state;
pub mod storage;

pub use api::{
    ConstructedOperator, MessageOperator, Registry, SourceEvent, SourceOffset, SourceOperator,
    TaskContext,
};
pub use cluster::{
    CompileError, ExchangeMode, ExecutionGraph, JobCompiler, JobId, PartitioningStrategy,
    PhysicalEdgeDescriptor, ResourceProfile, SubtaskIndex, TaskDeploymentDescriptor, TaskManager,
    VertexId,
};
pub use error::RunError;
pub use execution::{SOURCE_IDLE_SLEEP, SourceRunner, SubtaskRunner};
pub use factory::{OperatorConstructor, OperatorFactory};
pub use memory::{MemoryPool, MemoryTicket};
pub use network::{BoxedEventStream, NetworkEnvironment, PhysicalSender, RemoteSenderStub};
pub use protocol::{
    CheckpointBarrierWire, ControlCommand, StopMode, StreamEvent, StreamOutput,
    control_channel, merge_watermarks, watermark_strictly_advances,
};
