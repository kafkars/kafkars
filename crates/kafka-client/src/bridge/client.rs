//! Facade-owned engine lifetime and private child-handle construction.

use kafka_client_engine::{
    ConsumerReadIsolation as EngineReadIsolation, Engine, EngineConfig, EngineStartErrorKind,
    ProducerCompression as EngineCompression,
};

use crate::consumer::ReadIsolation;
use crate::error::{ErrorKind, KafkaError};
use crate::producer::Compression;

/// Facade-owned handle that hides engine types from public modules.
#[derive(Debug, Clone)]
pub(crate) struct ClientEngine {
    inner: Engine,
}

impl ClientEngine {
    /// Starts the engine from facade-owned configuration values.
    pub(crate) fn start(
        bootstrap_servers: Vec<String>,
        compression: Compression,
        assigned_consumer_read_isolation: Option<ReadIsolation>,
    ) -> Result<Self, KafkaError> {
        let config = EngineConfig::new(bootstrap_servers)
            .with_producer_compression(engine_compression(compression));
        let config = match assigned_consumer_read_isolation {
            Some(read_isolation) => {
                config.with_assigned_consumer_read_isolation(engine_read_isolation(read_isolation))
            }
            None => config,
        };
        let inner = Engine::start(config).map_err(|error| {
            let kind = match error.kind() {
                EngineStartErrorKind::Configuration => ErrorKind::Configuration,
                EngineStartErrorKind::Admin
                | EngineStartErrorKind::Consumer
                | EngineStartErrorKind::Driver
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

    /// Returns an admin bridge with the engine-owned default timeout.
    pub(crate) fn admin(&self) -> super::admin::AdminEngine {
        super::admin::AdminEngine::new(self.inner.admin(), self.inner.config().admin_timeout())
    }

    /// Claims the engine's sole directly assigned consumer.
    pub(crate) fn claim_assigned_consumer(
        &self,
    ) -> Result<super::consumer::AssignedConsumerEngine, KafkaError> {
        super::consumer::AssignedConsumerEngine::claim(&self.inner)
    }
}

pub(super) const fn engine_read_isolation(read_isolation: ReadIsolation) -> EngineReadIsolation {
    match read_isolation {
        ReadIsolation::ReadUncommitted => EngineReadIsolation::ReadUncommitted,
        ReadIsolation::ReadCommitted => EngineReadIsolation::ReadCommitted,
    }
}

const fn engine_compression(compression: Compression) -> EngineCompression {
    match compression {
        Compression::None => EngineCompression::None,
        Compression::Gzip => EngineCompression::Gzip,
        Compression::Snappy => EngineCompression::Snappy,
        Compression::Lz4 => EngineCompression::Lz4,
        Compression::Zstd => EngineCompression::Zstd,
    }
}
