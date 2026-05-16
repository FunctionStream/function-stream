use std::error::Error;

pub type CatalogError = Box<dyn Error + Send + Sync + 'static>;
pub type CatalogResult<T> = Result<T, CatalogError>;
