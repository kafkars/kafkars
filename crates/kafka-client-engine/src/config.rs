//! Public engine defaults compiled before bounded host resources start.

use std::time::Duration;

mod classic_group_config;
mod compression;
#[cfg(test)]
mod compression_test;
mod consumer_fetch;
mod consumer_limits;
mod group_consumer_operations;
mod producer_limits;
mod read_isolation;
mod security;
mod transaction;
mod validated;
mod validation;
pub use classic_group_config::EngineClassicGroupConfig;
pub(crate) use classic_group_config::ValidatedClassicGroupConfig;
pub use compression::ProducerCompression;
pub use consumer_fetch::EngineConsumerFetchConfig;
pub(crate) use consumer_fetch::{ConsumerFetchConfigError, ValidatedConsumerFetchConfig};
pub use consumer_limits::EngineConsumerLimits;
pub(crate) use consumer_limits::{
    ConsumerLimitsError, ValidatedConsumerLimits, validate_consumer_fetch_envelope,
};
pub use group_consumer_operations::EngineGroupConsumerOperationConfig;
pub(crate) use group_consumer_operations::ValidatedGroupConsumerOperationConfig;
pub use producer_limits::EngineProducerLimits;
pub use read_isolation::ConsumerReadIsolation;
pub use security::{EngineSasl, EngineSaslMechanism, EngineSecurity, EngineTls};
pub(crate) use validated::ValidatedEngineConfig;
pub(crate) use validation::EngineConfigError;

#[cfg(test)]
mod classic_group_config_test;
#[cfg(test)]
mod consumer_fetch_test;
#[cfg(test)]
mod consumer_limits_test;
#[cfg(test)]
mod group_consumer_operations_test;
#[cfg(test)]
mod producer_limits_test;
#[cfg(test)]
mod read_isolation_test;
#[cfg(test)]
mod security_test;
#[cfg(test)]
mod transaction_test;
#[cfg(test)]
mod validation_test;

const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_ADMIN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PRODUCER_RETRIES: u32 = 3;
const DEFAULT_PRODUCER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_COMPRESSION_WORKERS: usize = 2;
const DEFAULT_TURN_BUDGET: usize = 64;

/// Engine construction inputs compiled before any host thread starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    delivery_timeout: Duration,
    admin_timeout: Duration,
    producer_limits: EngineProducerLimits,
    producer_compression: ProducerCompression,
    assigned_consumer_read_isolation: ConsumerReadIsolation,
    security: EngineSecurity,
    assigned_consumer_fetch: EngineConsumerFetchConfig,
    assigned_consumer_limits: EngineConsumerLimits,
    producer_retry_max: u32,
    producer_retry_backoff: Duration,
}

impl EngineConfig {
    /// Creates an engine configuration with engine-owned execution defaults.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self {
            bootstrap_servers,
            client_id: None,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            admin_timeout: DEFAULT_ADMIN_TIMEOUT,
            producer_limits: EngineProducerLimits::default(),
            producer_compression: ProducerCompression::None,
            assigned_consumer_read_isolation: ConsumerReadIsolation::default(),
            security: EngineSecurity::default(),
            assigned_consumer_fetch: EngineConsumerFetchConfig::default(),
            assigned_consumer_limits: EngineConsumerLimits::default(),
            producer_retry_max: DEFAULT_PRODUCER_RETRIES,
            producer_retry_backoff: DEFAULT_PRODUCER_RETRY_BACKOFF,
        }
    }

    /// Replaces the end-to-end default producer delivery timeout.
    #[must_use]
    pub const fn with_delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.delivery_timeout = delivery_timeout;
        self
    }

    /// Replaces the end-to-end default admin operation timeout.
    #[must_use]
    pub const fn with_admin_timeout(mut self, admin_timeout: Duration) -> Self {
        self.admin_timeout = admin_timeout;
        self
    }

    /// Replaces the provisional bounded producer resource contract.
    #[must_use]
    pub const fn with_producer_limits(mut self, producer_limits: EngineProducerLimits) -> Self {
        self.producer_limits = producer_limits;
        self
    }

    /// Selects the `RecordBatch` compression policy before the host starts.
    #[must_use]
    pub const fn with_producer_compression(
        mut self,
        producer_compression: ProducerCompression,
    ) -> Self {
        self.producer_compression = producer_compression;
        self
    }

    /// Selects immutable record visibility for the sole assigned consumer.
    #[must_use]
    pub const fn with_assigned_consumer_read_isolation(
        mut self,
        read_isolation: ConsumerReadIsolation,
    ) -> Self {
        self.assigned_consumer_read_isolation = read_isolation;
        self
    }

    /// Replaces the complete transport and broker-authentication policy.
    #[must_use]
    pub fn with_security(mut self, security: EngineSecurity) -> Self {
        self.security = security;
        self
    }

    /// Replaces the optional identity written into every Kafka request header.
    #[must_use]
    pub fn with_client_id(mut self, client_id: Option<String>) -> Self {
        self.client_id = client_id;
        self
    }

    /// Replaces the sole assigned consumer's broker Fetch policy.
    #[must_use]
    pub const fn with_assigned_consumer_fetch(mut self, fetch: EngineConsumerFetchConfig) -> Self {
        self.assigned_consumer_fetch = fetch;
        self
    }

    /// Replaces the sole assigned consumer's bounded resource capacities.
    #[must_use]
    pub const fn with_assigned_consumer_limits(mut self, limits: EngineConsumerLimits) -> Self {
        self.assigned_consumer_limits = limits;
        self
    }

    /// Replaces bounded definitely-unsent retry intent.
    #[must_use]
    pub const fn with_producer_retry(mut self, max_retries: u32, backoff: Duration) -> Self {
        self.producer_retry_max = max_retries;
        self.producer_retry_backoff = backoff;
        self
    }

    /// Returns configured logical bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// Returns the immutable request-header identity, when configured.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the engine-owned default delivery timeout.
    pub const fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
    }

    /// Returns the engine-owned default admin operation timeout.
    pub const fn admin_timeout(&self) -> Duration {
        self.admin_timeout
    }

    /// Returns the provisional bounded producer limits.
    pub const fn producer_limits(&self) -> EngineProducerLimits {
        self.producer_limits
    }

    /// Returns the configured producer compression policy.
    pub const fn producer_compression(&self) -> ProducerCompression {
        self.producer_compression
    }

    /// Returns immutable record visibility for the sole assigned consumer.
    pub const fn assigned_consumer_read_isolation(&self) -> ConsumerReadIsolation {
        self.assigned_consumer_read_isolation
    }

    /// Returns the complete transport and broker-authentication policy.
    pub const fn security(&self) -> &EngineSecurity {
        &self.security
    }

    /// Returns the sole assigned consumer's raw broker Fetch policy.
    pub const fn assigned_consumer_fetch(&self) -> EngineConsumerFetchConfig {
        self.assigned_consumer_fetch
    }

    /// Returns the sole assigned consumer's raw resource capacities.
    pub const fn assigned_consumer_limits(&self) -> EngineConsumerLimits {
        self.assigned_consumer_limits
    }

    /// Returns the maximum definitely-unsent retries per producer batch.
    pub const fn producer_retry_max(&self) -> u32 {
        self.producer_retry_max
    }

    /// Returns the fixed delay between definitely-unsent producer attempts.
    pub const fn producer_retry_backoff(&self) -> Duration {
        self.producer_retry_backoff
    }
}
