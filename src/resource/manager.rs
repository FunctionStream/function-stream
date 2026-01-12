// Resource manager implementation

use crate::resource::types::{Resource, ResourceId, ResourceMetadata};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Resource manager
pub struct ResourceManager {
    /// Registered resources: resource_id -> resource
    resources: Arc<RwLock<HashMap<ResourceId, Box<dyn Resource>>>>,
}

impl ResourceManager {
    /// Create a new resource manager
    pub fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a resource
    pub async fn register(&self, resource: Box<dyn Resource>) -> Result<()> {
        let id = resource.id().clone();
        let mut resources = self.resources.write().await;
        
        if resources.contains_key(&id) {
            return Err(anyhow::anyhow!("Resource with ID '{}' already exists", id));
        }

        resources.insert(id.clone(), resource);
        log::info!("Registering resource: {}", id);
        Ok(())
    }

    /// Get resource metadata by ID
    pub async fn get_metadata(&self, id: &ResourceId) -> Option<ResourceMetadata> {
        let resources = self.resources.read().await;
        resources.get(id).map(|r| r.metadata().clone())
    }

    /// Check if a resource exists
    pub async fn exists(&self, id: &ResourceId) -> bool {
        let resources = self.resources.read().await;
        resources.contains_key(id)
    }

    /// List all resource IDs
    pub async fn list_ids(&self) -> Vec<ResourceId> {
        let resources = self.resources.read().await;
        resources.keys().cloned().collect()
    }


    /// Unregister a resource
    pub async fn unregister(&self, id: &ResourceId) -> Result<()> {
        let mut resources = self.resources.write().await;
        
        if let Some(mut resource) = resources.remove(id) {
            log::info!("Unregistering resource: {}", id);
            // Cleanup the resource
            if let Err(e) = resource.cleanup().await {
                log::error!("Error cleaning up resource {}: {}", id, e);
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Resource with ID '{}' not found", id))
        }
    }

    /// Cleanup all resources
    pub async fn cleanup_all(&self) -> Result<()> {
        let mut resources = self.resources.write().await;
        let ids: Vec<ResourceId> = resources.keys().cloned().collect();
        
        for id in ids {
            if let Some(mut resource) = resources.remove(&id) {
                log::info!("Cleaning up resource: {}", id);
                if let Err(e) = resource.cleanup().await {
                    log::error!("Error cleaning up resource {}: {}", id, e);
                }
            }
        }
        
        Ok(())
    }

    /// Get resource count
    pub async fn count(&self) -> usize {
        let resources = self.resources.read().await;
        resources.len()
    }
}

impl Default for ResourceManager {
    fn default() -> Self {
        Self::new()
    }
}

