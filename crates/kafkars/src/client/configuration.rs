//! Inert cluster, security, consumer, and producer policy selection.

use std::time::Duration;

use crate::consumer::{ConsumerFetchConfig, ConsumerLimits, ReadIsolation};
use crate::producer::{Compression, ProducerConfig, ProducerLimits, ProducerRetryConfig};
use crate::security::Security;

use super::ClientBuilder;

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
    /// proof fails. Later [`super::Client::ready`] calls repeat the point-in-time
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

    /// Selects `RecordBatch` compression for this client's producer owner.
    #[must_use]
    pub const fn producer_compression(mut self, compression: Compression) -> Self {
        self.producer = self.producer.with_compression(compression);
        self
    }

    /// Sets independent active, waiting, and batch producer ownership bounds.
    #[must_use]
    pub const fn producer_limits(mut self, limits: ProducerLimits) -> Self {
        self.producer = self.producer.with_limits(limits);
        self
    }

    /// Sets bounded safe record-execution and transaction-request replacements.
    ///
    /// A zero retry count disables retries and ignores the backoff value.
    #[must_use]
    pub const fn producer_retry(mut self, max_retries: u32, backoff: Duration) -> Self {
        self.producer = self
            .producer
            .with_retry(ProducerRetryConfig::new(max_retries, backoff));
        self
    }

    /// Sets the complete producer policy fixed before the client host starts.
    ///
    /// Ordinary and transactional record execution remain idempotent with
    /// `acks=all` and never request automatic topic creation. This config
    /// controls the ordinary producer's default delivery duration and their
    /// shared compression, safe retry, and bounded resource policy without
    /// exposing durability downgrade switches.
    #[must_use]
    pub const fn producer_config(mut self, config: ProducerConfig) -> Self {
        self.producer = config;
        self
    }

    /// Replaces the default delivery duration inherited by producer builders.
    #[must_use]
    pub const fn producer_delivery_timeout(mut self, timeout: Duration) -> Self {
        self.producer = self.producer.with_delivery_timeout(timeout);
        self
    }

    /// Returns the complete selected producer policy.
    pub const fn selected_producer_config(&self) -> ProducerConfig {
        self.producer
    }

    /// Returns the default delivery duration inherited by producer builders.
    pub const fn selected_producer_delivery_timeout(&self) -> Duration {
        self.producer.delivery_timeout()
    }

    /// Returns the selected producer compression policy.
    pub const fn selected_producer_compression(&self) -> Compression {
        self.producer.compression()
    }

    /// Returns the selected producer resource contract.
    pub const fn selected_producer_limits(&self) -> ProducerLimits {
        self.producer.limits()
    }

    /// Returns the selected safe retry contract.
    pub const fn selected_producer_retry(&self) -> ProducerRetryConfig {
        self.producer.retry()
    }
}
