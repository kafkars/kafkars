//! Facade-owned engine lifetime and private child-handle construction.

mod configuration;
mod identity;
pub(crate) mod metrics;
mod share_consumer;

use std::sync::Arc;

use kafka_client_engine::{Engine, EngineConfig, EngineStartErrorKind, GroupConsumerStartCapture};

use crate::consumer::{
    ClassicGroupAssignor, ClassicGroupConfig, ConsumerFetchConfig, ConsumerGroupProtocol,
    ConsumerLimits, GroupConsumerOperationConfig, OffsetReset, ReadIsolation,
};
use crate::error::{Error as KafkaError, ErrorKind};
use crate::producer::ProducerConfig;
use crate::security::Security;
use crate::shutdown::Shutdown;

use super::consumer_configuration::{
    engine_consumer_fetch, engine_consumer_limits, engine_read_isolation,
};
pub(super) use configuration::engine_security;
use configuration::{engine_compression, engine_producer_limits};

/// Facade-owned handle that hides engine types from public modules.
#[derive(Debug, Clone)]
pub(crate) struct ClientEngine {
    inner: Engine,
    shutdown: super::client_shutdown::ClientShutdownOwner,
    expected_cluster_id: Option<Arc<str>>,
}

impl ClientEngine {
    /// Starts the engine from facade-owned configuration values.
    #[expect(
        clippy::needless_pass_by_value,
        clippy::too_many_arguments,
        reason = "the consuming builder transfers every explicit policy and its exact security owner"
    )]
    pub(crate) fn start_with_consumer_fetch(
        bootstrap_servers: Vec<String>,
        client_id: Option<String>,
        security: Security,
        producer: ProducerConfig,
        assigned_consumer_read_isolation: Option<ReadIsolation>,
        assigned_consumer_fetch: ConsumerFetchConfig,
        assigned_consumer_limits: ConsumerLimits,
        expected_cluster_id: Option<String>,
        identity_deadline: Option<std::time::Instant>,
    ) -> Result<Self, KafkaError> {
        let (delivery_timeout, compression, retry, producer_limits) = producer.into_parts();
        let config = EngineConfig::new(bootstrap_servers)
            .with_client_id(client_id)
            .with_security(engine_security(&security))
            .with_delivery_timeout(delivery_timeout)
            .with_producer_compression(engine_compression(compression))
            .with_producer_limits(engine_producer_limits(producer_limits))
            .with_producer_retry(retry.max_retries(), retry.backoff());
        let config = match assigned_consumer_read_isolation {
            Some(read_isolation) => {
                config.with_assigned_consumer_read_isolation(engine_read_isolation(read_isolation))
            }
            None => config,
        }
        .with_assigned_consumer_fetch(engine_consumer_fetch(assigned_consumer_fetch))
        .with_assigned_consumer_limits(engine_consumer_limits(assigned_consumer_limits));
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
        let client = Self {
            inner,
            shutdown,
            expected_cluster_id: expected_cluster_id.map(Arc::from),
        };
        identity::verify_startup(client, identity_deadline)
    }

    /// Returns the validated logical bootstrap endpoints.
    pub(crate) fn bootstrap_servers(&self) -> &[String] {
        self.inner.config().bootstrap_servers()
    }

    /// Returns the immutable request-header identity retained by the engine.
    pub(crate) fn client_id(&self) -> Option<&str> {
        self.inner.config().client_id()
    }

    #[cfg(test)]
    pub(crate) fn producer_retry(&self) -> (u32, std::time::Duration) {
        let config = self.inner.config();
        (config.producer_retry_max(), config.producer_retry_backoff())
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
    #[expect(
        clippy::too_many_arguments,
        reason = "the bridge forwards each explicit group policy without a second configuration owner"
    )]
    pub(crate) fn register_group_consumer(
        &self,
        capture: GroupConsumerStartCapture,
        group: &str,
        group_instance_id: Option<&str>,
        topics: &[String],
        group_protocol: ConsumerGroupProtocol,
        classic_group_assignor: Option<ClassicGroupAssignor>,
        offset_reset: OffsetReset,
        read_isolation: ReadIsolation,
        processing_timeout: std::time::Duration,
        classic_group_config: ClassicGroupConfig,
        operations: GroupConsumerOperationConfig,
        fetch: ConsumerFetchConfig,
        limits: ConsumerLimits,
    ) -> Result<super::consumer_facade::group_consumer::GroupConsumerEngine, KafkaError> {
        super::consumer_facade::group_consumer::GroupConsumerEngine::register_with_fetch(
            &self.inner,
            capture,
            group,
            group_instance_id,
            topics,
            group_protocol,
            classic_group_assignor,
            offset_reset,
            read_isolation,
            processing_timeout,
            classic_group_config,
            operations,
            fetch,
            limits,
        )
    }
}
