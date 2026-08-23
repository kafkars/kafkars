//! Public `ShareFetch` request policy fixed before a share member starts.

use std::time::Duration;

/// Broker long-poll, acquisition, and attempt settings for a share consumer.
///
/// `max_bytes` and `max_records` are Kafka's soft request bounds: KIP-74 and
/// record-batch alignment can exceed them. Hard retained response and delivery
/// limits remain engine-owned until the bounded delivery store is configured.
/// Defaults are 500 ms, one byte, one MiB, 500 records, 500 records per
/// acquired range, and a 30-second attempt timeout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchConfig {
    max_wait: Duration,
    min_bytes: usize,
    max_bytes: usize,
    max_records: usize,
    batch_size: usize,
    attempt_timeout: Duration,
}

impl ShareConsumerFetchConfig {
    /// Creates one explicit `ShareFetch` policy.
    pub const fn new(
        max_wait: Duration,
        min_bytes: usize,
        max_bytes: usize,
        max_records: usize,
        batch_size: usize,
        attempt_timeout: Duration,
    ) -> Self {
        Self {
            max_wait,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            attempt_timeout,
        }
    }

    /// Returns the broker's maximum long-poll interval.
    pub const fn max_wait(self) -> Duration {
        self.max_wait
    }

    /// Returns the preferred minimum response bytes.
    pub const fn min_bytes(self) -> usize {
        self.min_bytes
    }

    /// Returns the soft whole-response byte ceiling sent to Kafka.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the soft acquired-record ceiling sent to Kafka.
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    /// Returns Kafka's preferred acquired-range batch size.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the end-to-end timeout for each background `ShareFetch` attempt.
    pub const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }

    /// Replaces the broker's maximum long-poll interval.
    #[must_use]
    pub const fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Replaces the preferred minimum response bytes.
    #[must_use]
    pub const fn with_min_bytes(mut self, min_bytes: usize) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    /// Replaces the soft whole-response byte ceiling sent to Kafka.
    #[must_use]
    pub const fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Replaces the soft acquired-record ceiling sent to Kafka.
    #[must_use]
    pub const fn with_max_records(mut self, max_records: usize) -> Self {
        self.max_records = max_records;
        self
    }

    /// Replaces Kafka's preferred acquired-range batch size.
    #[must_use]
    pub const fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Replaces the end-to-end timeout for each background `ShareFetch` attempt.
    #[must_use]
    pub const fn with_attempt_timeout(mut self, attempt_timeout: Duration) -> Self {
        self.attempt_timeout = attempt_timeout;
        self
    }

    pub(crate) const fn into_parts(self) -> (Duration, usize, usize, usize, usize, Duration) {
        (
            self.max_wait,
            self.min_bytes,
            self.max_bytes,
            self.max_records,
            self.batch_size,
            self.attempt_timeout,
        )
    }
}

impl Default for ShareConsumerFetchConfig {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(500),
            1,
            1024 * 1024,
            500,
            500,
            Duration::from_secs(30),
        )
    }
}
