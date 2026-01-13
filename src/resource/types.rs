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

// Resource types and definitions

use std::sync::Arc;
use tokio::sync::RwLock;

/// Resource identifier
pub type ResourceId = String;

/// Resource state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceState {
    /// Resource is being initialized
    Initializing,
    /// Resource is ready to use
    Ready,
    /// Resource is in use
    InUse,
    /// Resource is being cleaned up
    CleaningUp,
    /// Resource has been released
    Released,
}

/// Resource metadata
#[derive(Debug, Clone)]
pub struct ResourceMetadata {
    /// Resource ID
    pub id: ResourceId,
    /// Resource name
    pub name: String,
    /// Resource type
    pub resource_type: String,
    /// Creation timestamp
    pub created_at: std::time::SystemTime,
    /// Last access timestamp
    pub last_accessed: std::time::SystemTime,
}

/// Resource trait
/// Note: Using async-trait for async methods
#[async_trait::async_trait]
pub trait Resource: Send + Sync + 'static {
    /// Get resource ID
    fn id(&self) -> &ResourceId;

    /// Get resource metadata
    fn metadata(&self) -> &ResourceMetadata;

    /// Get resource state
    fn state(&self) -> ResourceState;

    /// Initialize the resource
    async fn initialize(&mut self) -> anyhow::Result<()>;

    /// Cleanup the resource
    async fn cleanup(&mut self) -> anyhow::Result<()>;
}

/// Generic resource wrapper
pub struct ResourceWrapper<T> {
    pub metadata: ResourceMetadata,
    pub state: Arc<RwLock<ResourceState>>,
    pub inner: Arc<RwLock<T>>,
}

impl<T> ResourceWrapper<T> {
    /// Create a new resource wrapper
    pub fn new(id: ResourceId, name: String, resource_type: String, inner: T) -> Self {
        Self {
            metadata: ResourceMetadata {
                id: id.clone(),
                name,
                resource_type,
                created_at: std::time::SystemTime::now(),
                last_accessed: std::time::SystemTime::now(),
            },
            state: Arc::new(RwLock::new(ResourceState::Initializing)),
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Update last accessed time
    pub fn touch(&mut self) {
        self.metadata.last_accessed = std::time::SystemTime::now();
    }
}
