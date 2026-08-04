//! Public broker Fetch policy fixed before a consumer starts.

use std::time::Duration;

/// Broker long-poll and byte policy shared by assigned and group consumers.
///
/// These limits shape each Kafka Fetch request. Retained delivery capacity is
/// configured separately because broker soft byte limits do not authorize the
/// client to retain an oversized response.
///
/// The default uses a 500 ms broker wait, one-byte minimum, one-MiB response
/// and partition ceilings, and a 30-second end-to-end attempt timeout.
/// Consumer startup rejects non-whole-millisecond waits, out-of-range Kafka
/// byte fields, incoherent minimum/maximum bytes, and zero timeouts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerFetchConfig {
    max_wait: Duration,
    min_bytes: usize,
    max_bytes: usize,
    partition_max_bytes: usize,
    attempt_timeout: Duration,
}

impl ConsumerFetchConfig {
    /// Creates one explicit Fetch policy.
    pub const fn new(
        max_wait: Duration,
        min_bytes: usize,
        max_bytes: usize,
        partition_max_bytes: usize,
        attempt_timeout: Duration,
    ) -> Self {
        Self {
            max_wait,
            min_bytes,
            max_bytes,
            partition_max_bytes,
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

    /// Returns the soft per-partition byte ceiling sent to Kafka.
    pub const fn partition_max_bytes(self) -> usize {
        self.partition_max_bytes
    }

    /// Returns the end-to-end deadline for each background Fetch attempt.
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

    /// Replaces the soft per-partition byte ceiling sent to Kafka.
    #[must_use]
    pub const fn with_partition_max_bytes(mut self, partition_max_bytes: usize) -> Self {
        self.partition_max_bytes = partition_max_bytes;
        self
    }

    /// Replaces the end-to-end deadline for each background Fetch attempt.
    #[must_use]
    pub const fn with_attempt_timeout(mut self, attempt_timeout: Duration) -> Self {
        self.attempt_timeout = attempt_timeout;
        self
    }

    pub(crate) const fn into_parts(self) -> (Duration, usize, usize, usize, Duration) {
        (
            self.max_wait,
            self.min_bytes,
            self.max_bytes,
            self.partition_max_bytes,
            self.attempt_timeout,
        )
    }
}

impl Default for ConsumerFetchConfig {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(500),
            1,
            1024 * 1024,
            1024 * 1024,
            Duration::from_secs(30),
        )
    }
}
