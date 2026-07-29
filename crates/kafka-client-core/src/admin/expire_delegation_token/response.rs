//! Validated protocol-normalized successful API-40 response facts.

use core::fmt;

/// Successful broker fields before terminal assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenResponse {
    throttle_time_ms: u32,
    expiry_timestamp_ms: i64,
}

impl ExpireDelegationTokenResponse {
    /// Validates one nonnegative token expiry timestamp.
    pub const fn new(
        throttle_time_ms: u32,
        expiry_timestamp_ms: i64,
    ) -> Result<Self, ExpireDelegationTokenResponseError> {
        if expiry_timestamp_ms < 0 {
            return Err(ExpireDelegationTokenResponseError::NegativeExpiryTimestamp);
        }
        Ok(Self {
            throttle_time_ms,
            expiry_timestamp_ms,
        })
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the nonnegative expiry epoch timestamp.
    pub const fn expiry_timestamp_ms(self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Consumes the response into adapter-independent scalar parts.
    pub const fn into_parts(self) -> (u32, i64) {
        (self.throttle_time_ms, self.expiry_timestamp_ms)
    }
}

/// Invalid protocol-normalized successful response facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenResponseError {
    /// Kafka reported a negative expiry timestamp on success.
    NegativeExpiryTimestamp,
}

impl fmt::Display for ExpireDelegationTokenResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid ExpireDelegationToken response: {self:?}"
        )
    }
}

impl std::error::Error for ExpireDelegationTokenResponseError {}
