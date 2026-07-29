//! Engine-owned inert secret and expiration-period intent for API key 40.

use core::fmt;

use kafka_client_core::{
    ExpireDelegationTokenHmac as CoreHmac, ExpireDelegationTokenPlan as CorePlan,
};
use zeroize::Zeroize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpireDelegationTokenPlanFailure {
    Invalid,
    RetainedBytes,
}

/// Unique delegation-token HMAC ownership with redacted diagnostics.
#[derive(Eq, PartialEq)]
pub struct ExpireDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl ExpireDelegationTokenHmac {
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

    fn into_core(self) -> Result<CoreHmac, ExpireDelegationTokenPlanFailure> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.bytes.len())
            .map_err(|_| ExpireDelegationTokenPlanFailure::RetainedBytes)?;
        bytes.extend_from_slice(&self.bytes);
        CoreHmac::new(bytes).map_err(|_error| ExpireDelegationTokenPlanFailure::Invalid)
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.bytes.zeroize();
    }
}

impl Drop for ExpireDelegationTokenHmac {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl fmt::Debug for ExpireDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// One inert delegation-token expiration request.
#[derive(Debug, Eq, PartialEq)]
pub struct ExpireDelegationTokenRequest {
    hmac: ExpireDelegationTokenHmac,
    expiry_period_ms: Option<u64>,
}

impl ExpireDelegationTokenRequest {
    /// Creates inert expiration intent. Absence means Kafka's immediate `-1` sentinel.
    pub const fn new(hmac: ExpireDelegationTokenHmac, expiry_period_ms: Option<u64>) -> Self {
        Self {
            hmac,
            expiry_period_ms,
        }
    }

    /// Borrows the unique token HMAC.
    pub const fn hmac(&self) -> &ExpireDelegationTokenHmac {
        &self.hmac
    }

    /// Returns the explicit nonnegative expiration period in milliseconds.
    pub const fn expiry_period_ms(&self) -> Option<u64> {
        self.expiry_period_ms
    }

    /// Consumes this request into the unique HMAC and exact period intent.
    pub fn into_parts(self) -> (ExpireDelegationTokenHmac, Option<u64>) {
        (self.hmac, self.expiry_period_ms)
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, ExpireDelegationTokenPlanFailure> {
        let (hmac, expiry_period_ms) = self.into_parts();
        let expiry_period_ms = expiry_period_ms
            .map(i64::try_from)
            .transpose()
            .map_err(|_error| ExpireDelegationTokenPlanFailure::Invalid)?;
        let hmac = hmac.into_core()?;
        CorePlan::new(hmac, expiry_period_ms)
            .map_err(|_error| ExpireDelegationTokenPlanFailure::Invalid)
    }
}
