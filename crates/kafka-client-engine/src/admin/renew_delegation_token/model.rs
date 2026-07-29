//! Engine-owned inert secret and renewal-period intent for API key 39.

use core::fmt;

use kafka_client_core::{
    RenewDelegationTokenHmac as CoreHmac, RenewDelegationTokenPlan as CorePlan,
};
use zeroize::Zeroize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenPlanFailure {
    Invalid,
    RetainedBytes,
}

/// Unique delegation-token HMAC ownership with redacted diagnostics.
#[derive(Eq, PartialEq)]
pub struct RenewDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl RenewDelegationTokenHmac {
    /// Creates inert secret ownership without beginning an operation.
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Borrows the exact token HMAC bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers unique ownership of the token HMAC bytes.
    pub fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.bytes)
    }

    fn into_core(self) -> Result<CoreHmac, RenewDelegationTokenPlanFailure> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.bytes.len())
            .map_err(|_| RenewDelegationTokenPlanFailure::RetainedBytes)?;
        bytes.extend_from_slice(&self.bytes);
        CoreHmac::new(bytes).map_err(|_error| RenewDelegationTokenPlanFailure::Invalid)
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.bytes.zeroize();
    }
}

impl Drop for RenewDelegationTokenHmac {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl fmt::Debug for RenewDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// One inert delegation-token renewal request.
#[derive(Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenRequest {
    hmac: RenewDelegationTokenHmac,
    renew_period_ms: Option<u64>,
}

impl RenewDelegationTokenRequest {
    /// Creates inert renewal intent. Absence of a period uses Kafka's default.
    pub const fn new(hmac: RenewDelegationTokenHmac, renew_period_ms: Option<u64>) -> Self {
        Self {
            hmac,
            renew_period_ms,
        }
    }

    /// Borrows the unique token HMAC.
    pub const fn hmac(&self) -> &RenewDelegationTokenHmac {
        &self.hmac
    }

    /// Returns the explicit positive renewal period in milliseconds.
    pub const fn renew_period_ms(&self) -> Option<u64> {
        self.renew_period_ms
    }

    /// Consumes this request into the unique HMAC and exact period intent.
    pub fn into_parts(self) -> (RenewDelegationTokenHmac, Option<u64>) {
        (self.hmac, self.renew_period_ms)
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, RenewDelegationTokenPlanFailure> {
        let (hmac, renew_period_ms) = self.into_parts();
        let renew_period_ms = renew_period_ms
            .map(i64::try_from)
            .transpose()
            .map_err(|_error| RenewDelegationTokenPlanFailure::Invalid)?;
        let hmac = hmac.into_core()?;
        CorePlan::new(hmac, renew_period_ms)
            .map_err(|_error| RenewDelegationTokenPlanFailure::Invalid)
    }
}
