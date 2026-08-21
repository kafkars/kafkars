//! Successful delegation-token expiration facts.

use std::time::Duration;

/// Kafka throttle and the expired token's absolute expiry timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenResult {
    throttle_time: Duration,
    expiry_timestamp_ms: i64,
}

impl ExpireDelegationTokenResult {
    pub(crate) const fn new(throttle_time: Duration, expiry_timestamp_ms: i64) -> Self {
        Self {
            throttle_time,
            expiry_timestamp_ms,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(self) -> Duration {
        self.throttle_time
    }

    /// Returns the token's Unix-epoch expiry timestamp in milliseconds.
    pub const fn expiry_timestamp_ms(self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Consumes the result into Kafka's throttle and expiry timestamp.
    pub const fn into_parts(self) -> (Duration, i64) {
        (self.throttle_time, self.expiry_timestamp_ms)
    }
}
