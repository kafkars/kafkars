//! Cluster-scoped client construction, readiness, and child-handle ownership.

use crate::admin::Admin;
use crate::bridge::ClientEngine;
use crate::consumer::{AssignedConsumerBuilder, ConsumerBuilder, ReadIsolation};
use crate::error::{ErrorKind, KafkaError};
use crate::metrics::Metrics;
use crate::producer::{Compression, ProducerBuilder, ProducerLimits};
use crate::readiness::Ready;
use crate::security::Security;
use crate::shutdown::Shutdown;
use crate::transaction::TransactionalProducerBuilder;

/// Builder for one shared cluster, security, and execution context.
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    security: Security,
    producer_compression: Compression,
    producer_limits: ProducerLimits,
    assigned_consumer_read_isolation: Option<ReadIsolation>,
}

impl ClientBuilder {
    /// Replaces the logical bootstrap endpoint set.
    pub fn bootstrap_servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bootstrap_servers = servers.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the client identifier reported to Kafka.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Selects transport encryption and broker authentication.
    #[must_use]
    pub fn security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    /// Selects `RecordBatch` compression for this client's producer owner.
    #[must_use]
    pub const fn producer_compression(mut self, compression: Compression) -> Self {
        self.producer_compression = compression;
        self
    }

    /// Sets independent active, waiting, and batch producer ownership bounds.
    #[must_use]
    pub const fn producer_limits(mut self, limits: ProducerLimits) -> Self {
        self.producer_limits = limits;
        self
    }

    /// Selects immutable record visibility for this client's assigned consumer.
    #[must_use]
    pub const fn assigned_consumer_read_isolation(mut self, read_isolation: ReadIsolation) -> Self {
        self.assigned_consumer_read_isolation = Some(read_isolation);
        self
    }

    /// Validates local configuration and starts the default host.
    pub fn build(self) -> Result<Client, KafkaError> {
        if self.bootstrap_servers.is_empty() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "at least one bootstrap server is required",
            ));
        }

        let engine = ClientEngine::start(
            self.bootstrap_servers,
            self.client_id,
            self.security,
            self.producer_compression,
            self.producer_limits,
            self.assigned_consumer_read_isolation,
        )?;
        Ok(Client { engine })
    }
}

/// Cheaply cloneable cluster-scoped client handle.
#[derive(Debug, Clone)]
pub struct Client {
    engine: ClientEngine,
}

impl Client {
    /// Begins client construction.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Returns the configured client identifier.
    pub fn client_id(&self) -> Option<&str> {
        self.engine.client_id()
    }

    /// Returns validated bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        self.engine.bootstrap_servers()
    }

    /// Probes broker readiness lazily through one bounded network operation.
    /// The independent probe's deadline starts at this call boundary.
    pub fn ready(&self) -> Ready {
        Ready::from_bridge(self.engine.ready())
    }

    /// Requests one bounded point-in-time operational metrics snapshot.
    pub fn metrics(&self) -> Result<Metrics, KafkaError> {
        self.engine.metrics().map(Metrics::from_bridge)
    }

    /// Begins construction of a thread-safe producer.
    pub fn producer(&self) -> ProducerBuilder {
        ProducerBuilder::new(self.engine.producer())
    }

    /// Begins construction of a uniquely controlled group consumer.
    pub fn consumer(&self, group_id: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::new(self.engine.clone(), group_id.into())
    }

    /// Begins construction of a directly assigned consumer.
    pub fn assigned_consumer(&self) -> AssignedConsumerBuilder {
        AssignedConsumerBuilder::new(self.engine.clone())
    }

    /// Returns a cheap thread-safe admin handle.
    pub fn admin(&self) -> Admin {
        Admin::new(self.engine.admin())
    }

    /// Begins construction of a uniquely controlled transactional producer.
    pub fn transactional_producer(
        &self,
        transactional_id: impl Into<String>,
    ) -> TransactionalProducerBuilder {
        TransactionalProducerBuilder::new(
            self.engine.transactional_producer(),
            transactional_id.into(),
        )
    }

    /// Initiates graceful client shutdown.
    pub fn shutdown(&self) -> Shutdown {
        self.engine.shutdown()
    }
}
