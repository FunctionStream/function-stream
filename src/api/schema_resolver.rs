use async_trait::async_trait;

/// Trait for resolving schemas by ID (e.g., from a schema registry).
#[async_trait]
pub trait SchemaResolver: Send {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String>;
}

/// A resolver that always fails — used when no schema registry is configured.
pub struct FailingSchemaResolver;

impl Default for FailingSchemaResolver {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl SchemaResolver for FailingSchemaResolver {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        Err(format!(
            "Schema with id {id} not available, and no schema registry configured"
        ))
    }
}

/// A resolver that returns a fixed schema for a known ID.
pub struct FixedSchemaResolver {
    id: u32,
    schema: String,
}

impl FixedSchemaResolver {
    pub fn new(id: u32, schema: String) -> Self {
        FixedSchemaResolver { id, schema }
    }
}

#[async_trait]
impl SchemaResolver for FixedSchemaResolver {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        if id == self.id {
            Ok(Some(self.schema.clone()))
        } else {
            Err(format!("Unexpected schema id {}, expected {}", id, self.id))
        }
    }
}

/// A caching wrapper around any `SchemaResolver`.
pub struct CachingSchemaResolver<R: SchemaResolver> {
    inner: R,
    cache: tokio::sync::RwLock<std::collections::HashMap<u32, String>>,
}

impl<R: SchemaResolver> CachingSchemaResolver<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            cache: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl<R: SchemaResolver + Sync> SchemaResolver for CachingSchemaResolver<R> {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        {
            let cache = self.cache.read().await;
            if let Some(schema) = cache.get(&id) {
                return Ok(Some(schema.clone()));
            }
        }

        let result = self.inner.resolve_schema(id).await?;
        if let Some(ref schema) = result {
            let mut cache = self.cache.write().await;
            cache.insert(id, schema.clone());
        }
        Ok(result)
    }
}
