//! Cluster-scoped client construction, readiness, and child-handle ownership.

use crate::admin::Admin;
use crate::bridge::ClientEngine;
use crate::consumer::{
    AssignedConsumerBuilder, ConsumerBuilder, ConsumerFetchConfig, ConsumerLimits, ReadIsolation,
    ShareConsumerBuilder,
};
use crate::error::{Error as KafkaError, ErrorKind};
use crate::metrics::Metrics;
use crate::producer::{ProducerBuilder, ProducerConfig};
use crate::security::Security;
use crate::transaction::TransactionalProducerBuilder;

pub use crate::readiness::Ready;
pub use crate::shutdown::Shutdown;

const CLUSTER_IDENTITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_EXPECTED_CLUSTER_ID_BYTES: usize = 1_024;

mod configuration;
#[cfg(test)]
mod configuration_test;

/// Builder for one shared cluster, security, and execution context.
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    expected_cluster_id: Option<String>,
    security: Security,
    producer: ProducerConfig,
    assigned_consumer_read_isolation: Option<ReadIsolation>,
    assigned_consumer_fetch: ConsumerFetchConfig,
    assigned_consumer_limits: ConsumerLimits,
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

    /// Sets the client identifier encoded in Kafka request headers.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Requires startup and every readiness probe to observe this exact cluster ID.
    ///
    /// Construction performs the first bounded `DescribeCluster` check before
    /// returning a usable client and shuts the started engine down if that
    /// proof fails. Later [`Client::ready`] calls repeat the point-in-time
    /// check; this option does not continuously monitor broker identity.
    #[must_use]
    pub fn expected_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.expected_cluster_id = Some(cluster_id.into());
        self
    }

    /// Selects transport encryption and broker authentication.
    #[must_use]
    pub fn security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    /// Selects immutable record visibility for this client's assigned consumer.
    #[must_use]
    pub const fn assigned_consumer_read_isolation(mut self, read_isolation: ReadIsolation) -> Self {
        self.assigned_consumer_read_isolation = Some(read_isolation);
        self
    }

    /// Sets the broker Fetch policy for this client's assigned consumer.
    #[must_use]
    pub const fn assigned_consumer_fetch(mut self, fetch: ConsumerFetchConfig) -> Self {
        self.assigned_consumer_fetch = fetch;
        self
    }

    /// Sets the resource capacities for this client's assigned consumer.
    #[must_use]
    pub const fn assigned_consumer_limits(mut self, limits: ConsumerLimits) -> Self {
        self.assigned_consumer_limits = limits;
        self
    }

    /// Validates local configuration and starts the default host.
    pub fn build(self) -> Result<Client, KafkaError> {
        let identity_deadline = self.expected_cluster_id.as_ref().map(|_| {
            cluster_identity_deadline_at(std::time::Instant::now()).ok_or_else(|| {
                KafkaError::new(
                    ErrorKind::Configuration,
                    "cluster identity deadline cannot be represented",
                )
            })
        });
        let identity_deadline = identity_deadline.transpose()?;
        if self.bootstrap_servers.is_empty() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "at least one bootstrap server is required",
            ));
        }
        if self.expected_cluster_id.as_deref() == Some("") {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "expected cluster ID must not be empty",
            ));
        }
        if self
            .expected_cluster_id
            .as_ref()
            .is_some_and(|cluster_id| cluster_id.len() > MAX_EXPECTED_CLUSTER_ID_BYTES)
        {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "expected cluster ID exceeds the 1024-byte limit",
            ));
        }

        let engine = ClientEngine::start_with_consumer_fetch(
            self.bootstrap_servers,
            self.client_id,
            self.security,
            self.producer,
            self.assigned_consumer_read_isolation,
            self.assigned_consumer_fetch,
            self.assigned_consumer_limits,
            self.expected_cluster_id,
            identity_deadline,
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

    /// Returns the configured client identifier passed to the driver for Kafka headers.
    pub fn client_id(&self) -> Option<&str> {
        self.engine.client_id()
    }

    /// Returns the exact broker-issued cluster ID required by this client.
    pub fn expected_cluster_id(&self) -> Option<&str> {
        self.engine.expected_cluster_id()
    }

    /// Returns validated bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        self.engine.bootstrap_servers()
    }

    /// Probes broker readiness lazily through one bounded network operation.
    ///
    /// The independent probe's deadline starts at this call boundary. When an
    /// expected cluster ID is configured, success also proves that exact ID at
    /// this point in time.
    pub fn ready(&self) -> Ready {
        let now = std::time::Instant::now();
        let deadline = cluster_identity_deadline_at(now).unwrap_or(now);
        Ready::from_bridge(self.engine.ready(deadline))
    }

    /// Requests one bounded operational metrics snapshot.
    ///
    /// Producer ownership is captured synchronously here. Driver counters are
    /// captured later by the reactor and are not cross-owner atomic with it.
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

    /// Begins construction of a unique Kafka share-group consumer.
    pub fn share_consumer(&self, group_id: impl Into<String>) -> ShareConsumerBuilder {
        ShareConsumerBuilder::new(self.engine.clone(), group_id.into())
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

pub(super) fn cluster_identity_deadline_at(
    boundary: std::time::Instant,
) -> Option<std::time::Instant> {
    boundary.checked_add(CLUSTER_IDENTITY_TIMEOUT)
}
