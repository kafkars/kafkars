//! Inert producer policy selection on the shared client builder.

use std::time::Duration;

use crate::producer::{Compression, ProducerConfig, ProducerLimits, ProducerRetryConfig};

use super::ClientBuilder;

impl ClientBuilder {
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
