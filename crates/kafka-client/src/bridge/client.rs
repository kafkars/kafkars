//! Facade-owned engine lifetime and private child-handle construction.

pub(crate) mod metrics;

use kafka_client_engine::{
    ConsumerReadIsolation as EngineReadIsolation, Engine, EngineConfig, EngineProducerLimits,
    EngineSasl, EngineSecurity, EngineStartErrorKind, EngineTls, GroupConsumerStartCapture,
    ProducerCompression as EngineCompression,
};

use crate::consumer::{OffsetReset, ReadIsolation};
use crate::error::{ErrorKind, KafkaError};
use crate::producer::{Compression, ProducerLimits};
use crate::security::{Sasl, SaslMechanism, Security};
use crate::shutdown::Shutdown;

/// Facade-owned handle that hides engine types from public modules.
#[derive(Debug, Clone)]
pub(crate) struct ClientEngine {
    inner: Engine,
    shutdown: super::client_shutdown::ClientShutdownOwner,
}

impl ClientEngine {
    /// Starts the engine from facade-owned configuration values.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the consuming client-builder boundary transfers its exact security owner"
    )]
    pub(crate) fn start(
        bootstrap_servers: Vec<String>,
        security: Security,
        compression: Compression,
        producer_limits: ProducerLimits,
        assigned_consumer_read_isolation: Option<ReadIsolation>,
    ) -> Result<Self, KafkaError> {
        let config = EngineConfig::new(bootstrap_servers)
            .with_security(engine_security(&security))
            .with_producer_compression(engine_compression(compression))
            .with_producer_limits(engine_producer_limits(producer_limits));
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
        let shutdown = super::client_shutdown::ClientShutdownOwner::try_new(inner.clone())?;
        Ok(Self { inner, shutdown })
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

    /// Immediately admits one bounded point-in-time readiness probe.
    pub(crate) fn ready(&self) -> super::admin_describe_operation::AdminDescribeCluster {
        self.admin()
            .submit_describe_cluster(self.inner.config().admin_timeout())
    }

    /// Starts or observes the one clone-shared terminal engine shutdown.
    pub(crate) fn shutdown(&self) -> Shutdown {
        Shutdown::from_bridge(self.shutdown.begin())
    }

    /// Returns the private transaction initializer retaining engine defaults.
    pub(crate) fn transactional_producer(
        &self,
    ) -> super::transaction::TransactionalProducerInitializer {
        super::transaction::TransactionalProducerInitializer::new(self.inner.clone())
    }

    /// Claims the engine's sole directly assigned consumer.
    pub(crate) fn claim_assigned_consumer(
        &self,
    ) -> Result<super::consumer::AssignedConsumerEngine, KafkaError> {
        super::consumer::AssignedConsumerEngine::claim(&self.inner)
    }

    /// Captures the membership deadline before facade validation or conversion.
    pub(crate) fn capture_group_consumer_start(
        &self,
        timeout: std::time::Duration,
    ) -> Result<GroupConsumerStartCapture, KafkaError> {
        self.inner.capture_group_consumer_start(timeout).map_err(
            super::consumer_facade::group_consumer_registration_result::translate_group_start,
        )
    }

    /// Registers one bounded dynamic classic-group owner and admits captured membership.
    pub(crate) fn register_group_consumer(
        &self,
        capture: GroupConsumerStartCapture,
        group: &str,
        group_instance_id: Option<&str>,
        topics: &[String],
        offset_reset: OffsetReset,
        read_isolation: ReadIsolation,
        processing_timeout: std::time::Duration,
    ) -> Result<super::consumer_facade::group_consumer::GroupConsumerEngine, KafkaError> {
        super::consumer_facade::group_consumer::GroupConsumerEngine::register(
            &self.inner,
            capture,
            group,
            group_instance_id,
            topics,
            offset_reset,
            read_isolation,
            processing_timeout,
        )
    }
}

pub(super) fn engine_security(security: &Security) -> EngineSecurity {
    match security {
        Security::Plaintext => EngineSecurity::plaintext(),
        Security::Tls(tls) => EngineSecurity::tls(engine_tls(tls)),
        Security::SaslPlaintext(sasl) => EngineSecurity::sasl_plaintext(engine_sasl(sasl)),
        Security::SaslTls { tls, sasl } => {
            EngineSecurity::sasl_tls(engine_tls(tls), engine_sasl(sasl))
        }
    }
}

fn engine_tls(tls: &crate::Tls) -> EngineTls {
    tls.custom_roots_pem_bytes()
        .map_or_else(EngineTls::system_roots, |pem| {
            EngineTls::custom_roots_pem(pem.to_vec())
        })
}

fn engine_sasl(sasl: &Sasl) -> EngineSasl {
    let (username, password) = sasl.credentials();
    match sasl.mechanism() {
        SaslMechanism::Plain => EngineSasl::plain(username, password),
        SaslMechanism::ScramSha256 => EngineSasl::scram_sha_256(username, password),
        SaslMechanism::ScramSha512 => EngineSasl::scram_sha_512(username, password),
    }
}

fn engine_producer_limits(limits: ProducerLimits) -> EngineProducerLimits {
    let (retained, active, waiting, waiting_bytes, batch, batch_bytes, linger) =
        limits.into_parts();
    EngineProducerLimits::new(
        retained,
        active,
        waiting,
        waiting_bytes,
        batch,
        batch_bytes,
        linger,
    )
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
