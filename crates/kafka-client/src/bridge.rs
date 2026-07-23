//! Private translation boundary between the Rust facade and the shared engine.

use kafka_client_engine::{Engine, EngineConfig};

/// Facade-owned handle that hides engine types from public modules.
#[derive(Debug, Clone)]
pub(crate) struct ClientEngine {
    inner: Engine,
}

impl ClientEngine {
    /// Starts the engine from facade-owned configuration values.
    pub(crate) fn start(bootstrap_servers: Vec<String>) -> Self {
        Self {
            inner: Engine::start(EngineConfig::new(bootstrap_servers)),
        }
    }

    /// Returns the validated logical bootstrap endpoints.
    pub(crate) fn bootstrap_servers(&self) -> &[String] {
        self.inner.config().bootstrap_servers()
    }
}
