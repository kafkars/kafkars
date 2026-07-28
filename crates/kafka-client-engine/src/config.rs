//! Public engine defaults compiled before bounded host resources start.

use std::time::Duration;

mod compression;
#[cfg(test)]
mod compression_test;
mod producer_limits;
mod read_isolation;
mod transaction;
mod validated;
mod validation;
pub use compression::ProducerCompression;
pub use producer_limits::EngineProducerLimits;
pub use read_isolation::ConsumerReadIsolation;
pub(crate) use validated::ValidatedEngineConfig;
pub(crate) use validation::EngineConfigError;

#[cfg(test)]
mod producer_limits_test;
#[cfg(test)]
mod read_isolation_test;
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
    delivery_timeout: Duration,
    admin_timeout: Duration,
    producer_limits: EngineProducerLimits,
    producer_compression: ProducerCompression,
    assigned_consumer_read_isolation: ConsumerReadIsolation,
    producer_retry_max: u32,
    producer_retry_backoff: Duration,
}

impl EngineConfig {
    /// Creates an engine configuration with engine-owned execution defaults.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self {
            bootstrap_servers,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            admin_timeout: DEFAULT_ADMIN_TIMEOUT,
            producer_limits: EngineProducerLimits::default(),
            producer_compression: ProducerCompression::None,
            assigned_consumer_read_isolation: ConsumerReadIsolation::default(),
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
}
