//! Facade-owned engine lifetime and private child-handle construction.

use kafka_client_engine::{Engine, EngineConfig, EngineStartErrorKind};

use crate::error::{ErrorKind, KafkaError};

/// Facade-owned handle that hides engine types from public modules.
#[derive(Debug, Clone)]
pub(crate) struct ClientEngine {
    inner: Engine,
}

impl ClientEngine {
    /// Starts the engine from facade-owned configuration values.
    pub(crate) fn start(bootstrap_servers: Vec<String>) -> Result<Self, KafkaError> {
        let inner = Engine::start(EngineConfig::new(bootstrap_servers)).map_err(|error| {
            let kind = match error.kind() {
                EngineStartErrorKind::Configuration => ErrorKind::Configuration,
                EngineStartErrorKind::Driver
                | EngineStartErrorKind::Producer
                | EngineStartErrorKind::HostThread
                | EngineStartErrorKind::HostHandoff => ErrorKind::Internal,
            };
            KafkaError::new(kind, error.to_string())
        })?;
        Ok(Self { inner })
    }

    /// Returns the validated logical bootstrap endpoints.
    pub(crate) fn bootstrap_servers(&self) -> &[String] {
        self.inner.config().bootstrap_servers()
    }

    /// Returns a producer bridge with the engine-owned default deadline.
    pub(crate) fn producer(&self) -> super::producer::ProducerEngine {
        super::producer::ProducerEngine::new(
            self.inner.producer(),
            self.inner.config().delivery_timeout(),
        )
    }
}
