//! Successful token expiration with exact throttle and expiry observations.

/// Fully settled successful expiration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenSuccess {
    throttle_time_ms: u32,
    expiry_timestamp_ms: i64,
}

impl ExpireDelegationTokenSuccess {
    pub(crate) const fn new(throttle_time_ms: u32, expiry_timestamp_ms: i64) -> Self {
        Self {
            throttle_time_ms,
            expiry_timestamp_ms,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the nonnegative expiry epoch timestamp.
    pub const fn expiry_timestamp_ms(self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Consumes success into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i64) {
        (self.throttle_time_ms, self.expiry_timestamp_ms)
    }
}
