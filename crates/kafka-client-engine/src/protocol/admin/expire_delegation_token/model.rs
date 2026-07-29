//! Generated-free expiration intent and normalized terminal facts.

use core::fmt;

/// Borrowed API-key 40 intent captured before owned secret materialization.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ExpireDelegationTokenRequestRef<'a> {
    hmac: &'a [u8],
    expiry_time_period_ms: i64,
    immediate: bool,
}

impl<'a> ExpireDelegationTokenRequestRef<'a> {
    /// Uses Kafka's exact `-1` immediate-expiration sentinel.
    pub(crate) const fn immediate(hmac: &'a [u8]) -> Self {
        Self {
            hmac,
            expiry_time_period_ms: -1,
            immediate: true,
        }
    }

    /// Retains one explicit nonnegative expiration period in milliseconds.
    pub(crate) const fn explicit(hmac: &'a [u8], expiry_time_period_ms: i64) -> Self {
        Self {
            hmac,
            expiry_time_period_ms,
            immediate: false,
        }
    }

    pub(crate) const fn hmac(self) -> &'a [u8] {
        self.hmac
    }

    pub(crate) const fn expiry_time_period_ms(self) -> i64 {
        self.expiry_time_period_ms
    }

    pub(super) const fn is_immediate(self) -> bool {
        self.immediate
    }
}

impl fmt::Debug for ExpireDelegationTokenRequestRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenRequestRef")
            .field("hmac", &"[REDACTED]")
            .field("expiry_time_period_ms", &self.expiry_time_period_ms)
            .field("immediate", &self.immediate)
            .finish()
    }
}

/// One bounded API-key 40 terminal preserving exact signed broker status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedExpireDelegationTokenResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    expiry_timestamp_ms: Option<i64>,
    retained_bytes: usize,
}

impl NormalizedExpireDelegationTokenResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        expiry_timestamp_ms: Option<i64>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            expiry_timestamp_ms,
            retained_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) const fn fixture(
        throttle_time_ms: u32,
        broker_error_code: i16,
        expiry_timestamp_ms: Option<i64>,
        retained_bytes: usize,
    ) -> Self {
        Self::new(
            throttle_time_ms,
            broker_error_code,
            expiry_timestamp_ms,
            retained_bytes,
        )
    }

    pub(crate) const fn into_parts(self) -> (u32, i16, Option<i64>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.expiry_timestamp_ms,
            self.retained_bytes,
        )
    }
}
