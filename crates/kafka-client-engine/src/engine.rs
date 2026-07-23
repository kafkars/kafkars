//! Shared client execution owner.

use std::sync::Arc;

use crate::EngineConfig;

/// Shared execution owner used by the curated facade.
#[derive(Debug, Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

#[derive(Debug)]
struct EngineInner {
    config: EngineConfig,
}

impl Engine {
    /// Starts the placeholder engine around validated local configuration.
    pub fn start(config: EngineConfig) -> Self {
        Self {
            inner: Arc::new(EngineInner { config }),
        }
    }

    /// Returns immutable engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }
}
