//! Generated-free renewal intent and normalized terminal facts.

/// Borrowed API-key 39 intent captured before owned secret materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RenewDelegationTokenRequestRef<'a> {
    hmac: &'a [u8],
    renew_period_ms: i64,
}

impl<'a> RenewDelegationTokenRequestRef<'a> {
    /// Uses Kafka's exact `-1` broker-default renewal-period sentinel.
    pub(crate) const fn broker_default(hmac: &'a [u8]) -> Self {
        Self {
            hmac,
            renew_period_ms: -1,
        }
    }

    /// Retains one explicit positive renewal period in milliseconds.
    pub(crate) const fn explicit(hmac: &'a [u8], renew_period_ms: i64) -> Self {
        Self {
            hmac,
            renew_period_ms,
        }
    }

    pub(crate) const fn hmac(self) -> &'a [u8] {
        self.hmac
    }

    pub(crate) const fn renew_period_ms(self) -> i64 {
        self.renew_period_ms
    }
}

/// One bounded API-key 39 terminal preserving exact signed broker status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedRenewDelegationTokenResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    expiry_timestamp_ms: Option<i64>,
    retained_bytes: usize,
}

impl NormalizedRenewDelegationTokenResponse {
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
