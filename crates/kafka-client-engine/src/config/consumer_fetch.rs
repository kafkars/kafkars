//! Public raw Fetch configuration and its validated engine representation.

use std::time::Duration;

/// Broker long-poll and byte policy compiled before a consumer starts.
///
/// Defaults are 500 ms, one byte, one MiB, one MiB, and 30 seconds respectively.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineConsumerFetchConfig {
    max_wait: Duration,
    min_bytes: usize,
    max_bytes: usize,
    partition_max_bytes: usize,
    attempt_timeout: Duration,
}

impl EngineConsumerFetchConfig {
    /// Creates raw Fetch settings for engine validation.
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

    /// Returns the soft whole-response byte ceiling.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the soft per-partition byte ceiling.
    pub const fn partition_max_bytes(self) -> usize {
        self.partition_max_bytes
    }

    /// Returns the end-to-end Fetch-attempt timeout.
    pub const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }

    pub(crate) fn validate(self) -> Result<ValidatedConsumerFetchConfig, ConsumerFetchConfigError> {
        let max_wait_ms =
            whole_positive_milliseconds(self.max_wait).ok_or(ConsumerFetchConfigError::MaxWait)?;
        let min_bytes =
            i32::try_from(self.min_bytes).map_err(|_error| ConsumerFetchConfigError::MinBytes)?;
        let max_bytes = i32::try_from(self.max_bytes)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ConsumerFetchConfigError::MaxBytes)?;
        let partition_max_bytes = i32::try_from(self.partition_max_bytes)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ConsumerFetchConfigError::PartitionMaxBytes)?;
        if min_bytes > max_bytes {
            return Err(ConsumerFetchConfigError::MinBytesExceedMaxBytes);
        }
        if self.attempt_timeout.is_zero() || u64::try_from(self.attempt_timeout.as_nanos()).is_err()
        {
            return Err(ConsumerFetchConfigError::AttemptTimeout);
        }
        Ok(ValidatedConsumerFetchConfig {
            max_wait_ms,
            min_bytes: u32::try_from(min_bytes)
                .unwrap_or_else(|_error| unreachable!("validated Fetch min bytes are nonnegative")),
            max_bytes: u32::try_from(max_bytes)
                .unwrap_or_else(|_error| unreachable!("validated Fetch max bytes are positive")),
            partition_max_bytes: u32::try_from(partition_max_bytes).unwrap_or_else(|_error| {
                unreachable!("validated partition Fetch max bytes are positive")
            }),
            attempt_timeout: self.attempt_timeout,
        })
    }
}

impl Default for EngineConsumerFetchConfig {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedConsumerFetchConfig {
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    partition_max_bytes: u32,
    attempt_timeout: Duration,
}

impl ValidatedConsumerFetchConfig {
    pub(crate) const fn max_wait_ms(self) -> u32 {
        self.max_wait_ms
    }

    pub(crate) const fn min_bytes(self) -> u32 {
        self.min_bytes
    }

    pub(crate) const fn max_bytes(self) -> u32 {
        self.max_bytes
    }

    pub(crate) const fn partition_max_bytes(self) -> u32 {
        self.partition_max_bytes
    }

    pub(crate) const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }
}

impl Default for ValidatedConsumerFetchConfig {
    fn default() -> Self {
        Self {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            attempt_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerFetchConfigError {
    MaxWait,
    MinBytes,
    MaxBytes,
    PartitionMaxBytes,
    MinBytesExceedMaxBytes,
    AttemptTimeout,
}

fn whole_positive_milliseconds(duration: Duration) -> Option<u32> {
    if duration.is_zero() || duration.subsec_nanos() % 1_000_000 != 0 {
        return None;
    }
    u32::try_from(duration.as_millis())
        .ok()
        .filter(|millis| i32::try_from(*millis).is_ok())
}
