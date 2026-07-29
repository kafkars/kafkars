//! Complete generated-free successful result values for token renewal.

/// Successful token renewal and Kafka's throttle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenResult {
    pub(super) throttle_time_ms: u32,
    pub(super) expiry_timestamp_ms: i64,
}

impl RenewDelegationTokenResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the renewed token expiry epoch timestamp.
    pub const fn expiry_timestamp_ms(self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Consumes success into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i64) {
        (self.throttle_time_ms, self.expiry_timestamp_ms)
    }
}
