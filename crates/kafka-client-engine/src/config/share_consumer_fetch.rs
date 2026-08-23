//! Public raw `ShareFetch` policy and its validated engine representation.

use std::time::Duration;

/// Broker long-poll, acquisition, and attempt policy fixed before a share member starts.
///
/// Defaults are a 500 ms wait, one byte minimum, one MiB response request,
/// 500 records, 500-record acquired batches, and a 30-second attempt timeout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineShareConsumerFetchConfig {
    max_wait: Duration,
    min_bytes: usize,
    max_bytes: usize,
    max_records: usize,
    batch_size: usize,
    attempt_timeout: Duration,
}

impl EngineShareConsumerFetchConfig {
    /// Creates raw `ShareFetch` settings for engine validation at registration.
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

    /// Returns the broker's maximum `ShareFetch` long-poll interval.
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

    /// Returns the soft record ceiling sent to Kafka.
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    /// Returns Kafka's preferred acquired-range batch size.
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the end-to-end timeout for one background `ShareFetch` attempt.
    pub const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }

    pub(crate) fn validate(
        self,
    ) -> Result<ValidatedShareConsumerFetchConfig, ShareConsumerFetchConfigError> {
        let max_wait_ms = whole_positive_milliseconds(self.max_wait)
            .ok_or(ShareConsumerFetchConfigError::MaxWait)?;
        let min_bytes = i32::try_from(self.min_bytes)
            .map_err(|_error| ShareConsumerFetchConfigError::MinBytes)?;
        let max_bytes = positive_i32(self.max_bytes, ShareConsumerFetchConfigError::MaxBytes)?;
        if min_bytes > max_bytes {
            return Err(ShareConsumerFetchConfigError::MinBytesExceedMaxBytes);
        }
        let max_records =
            positive_i32(self.max_records, ShareConsumerFetchConfigError::MaxRecords)?;
        let batch_size = positive_i32(self.batch_size, ShareConsumerFetchConfigError::BatchSize)?;
        if self.attempt_timeout.is_zero() || u64::try_from(self.attempt_timeout.as_nanos()).is_err()
        {
            return Err(ShareConsumerFetchConfigError::AttemptTimeout);
        }
        Ok(ValidatedShareConsumerFetchConfig {
            max_wait_ms,
            min_bytes: u32::try_from(min_bytes)
                .unwrap_or_else(|_error| unreachable!("validated ShareFetch min bytes")),
            max_bytes: u32::try_from(max_bytes)
                .unwrap_or_else(|_error| unreachable!("validated ShareFetch max bytes")),
            max_records: u32::try_from(max_records)
                .unwrap_or_else(|_error| unreachable!("validated ShareFetch max records")),
            batch_size: u32::try_from(batch_size)
                .unwrap_or_else(|_error| unreachable!("validated ShareFetch batch size")),
            attempt_timeout: self.attempt_timeout,
        })
    }
}

impl Default for EngineShareConsumerFetchConfig {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedShareConsumerFetchConfig {
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    max_records: u32,
    batch_size: u32,
    attempt_timeout: Duration,
}

impl ValidatedShareConsumerFetchConfig {
    #[cfg(test)]
    pub(crate) const fn max_wait_ms(self) -> u32 {
        self.max_wait_ms
    }

    #[cfg(test)]
    pub(crate) const fn min_bytes(self) -> u32 {
        self.min_bytes
    }

    #[cfg(test)]
    pub(crate) const fn max_bytes(self) -> u32 {
        self.max_bytes
    }

    #[cfg(test)]
    pub(crate) const fn max_records(self) -> u32 {
        self.max_records
    }

    #[cfg(test)]
    pub(crate) const fn batch_size(self) -> u32 {
        self.batch_size
    }

    #[cfg(test)]
    pub(crate) const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerFetchConfigError {
    MaxWait,
    MinBytes,
    MaxBytes,
    MaxRecords,
    BatchSize,
    MinBytesExceedMaxBytes,
    AttemptTimeout,
}

fn positive_i32(
    value: usize,
    error: ShareConsumerFetchConfigError,
) -> Result<i32, ShareConsumerFetchConfigError> {
    i32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(error)
}

fn whole_positive_milliseconds(duration: Duration) -> Option<u32> {
    if duration.is_zero() || duration.subsec_nanos() % 1_000_000 != 0 {
        return None;
    }
    u32::try_from(duration.as_millis())
        .ok()
        .filter(|millis| i32::try_from(*millis).is_ok())
}
