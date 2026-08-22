//! Public producer defaults and host-scoped policy fixed before client startup.

use std::time::Duration;

use super::{Compression, ProducerLimits, ProducerRetryConfig};

/// Complete configurable policy for the client's shared producer owner.
///
/// The beta producer is always idempotent and always sends with `acks=all`;
/// those durability guarantees cannot be silently weakened through this type.
/// Automatic topic creation is never requested. Compression, resource
/// ownership, and retry policy are fixed before the client host starts and are
/// shared by ordinary and transactional record execution. `delivery_timeout`
/// supplies the default end-to-end duration for each ordinary producer handle
/// and may be replaced on
/// [`ProducerBuilder`](super::ProducerBuilder) before that handle builds.
/// Defaults use a 30-second delivery duration, no compression, ten bounded
/// replacement attempts with 100-millisecond backoff, and
/// [`ProducerLimits::default`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerConfig {
    delivery_timeout: Duration,
    compression: Compression,
    retry: ProducerRetryConfig,
    limits: ProducerLimits,
}

impl ProducerConfig {
    /// Creates one explicit producer policy.
    pub const fn new(
        delivery_timeout: Duration,
        compression: Compression,
        retry: ProducerRetryConfig,
        limits: ProducerLimits,
    ) -> Self {
        Self {
            delivery_timeout,
            compression,
            retry,
            limits,
        }
    }

    /// Returns the default end-to-end duration for each accepted record.
    pub const fn delivery_timeout(self) -> Duration {
        self.delivery_timeout
    }

    /// Returns the `RecordBatch` compression policy.
    pub const fn compression(self) -> Compression {
        self.compression
    }

    /// Returns the bounded record-execution and transaction-request replacement policy.
    pub const fn retry(self) -> ProducerRetryConfig {
        self.retry
    }

    /// Returns active, waiting, and batching ownership limits.
    pub const fn limits(self) -> ProducerLimits {
        self.limits
    }

    /// Replaces the default end-to-end duration for each accepted record.
    #[must_use]
    pub const fn with_delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.delivery_timeout = delivery_timeout;
        self
    }

    /// Replaces the `RecordBatch` compression policy.
    #[must_use]
    pub const fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Replaces the bounded record-execution and transaction-request replacement policy.
    #[must_use]
    pub const fn with_retry(mut self, retry: ProducerRetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Replaces active, waiting, and batching ownership limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: ProducerLimits) -> Self {
        self.limits = limits;
        self
    }

    pub(crate) const fn into_parts(
        self,
    ) -> (Duration, Compression, ProducerRetryConfig, ProducerLimits) {
        (
            self.delivery_timeout,
            self.compression,
            self.retry,
            self.limits,
        )
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(30),
            Compression::None,
            ProducerRetryConfig::default(),
            ProducerLimits::default(),
        )
    }
}
