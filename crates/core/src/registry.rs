use crate::error::{VfsError, VfsResult};
use crate::vfs::VfsBackend;
use std::collections::HashMap;

/// Configuration parameters passed to a backend factory.
#[derive(Debug, Clone, Default)]
pub struct BackendConfig {
    pub params: HashMap<String, String>,
}

/// A function that creates a backend instance from config.
pub type BackendFactory = fn(BackendConfig) -> VfsResult<Box<dyn VfsBackend>>;

/// Registry of available backend factories (AGFS ServicePlugin pattern).
#[derive(Default)]
pub struct BackendRegistry {
    factories: HashMap<String, BackendFactory>,
}

impl BackendRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a backend factory under the given name.
    pub fn register(&mut self, name: &str, factory: BackendFactory) {
        self.factories.insert(name.to_string(), factory);
    }

    /// Create a backend instance by name.
    pub fn create(&self, name: &str, config: BackendConfig) -> VfsResult<Box<dyn VfsBackend>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| VfsError::Other(format!("Unknown backend: {name}")))?;
        factory(config)
    }

    /// List registered backend names.
    pub fn list(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }
}
