//! Deterministically ordered complete delegation-token result.

use super::super::DelegationToken;

/// Successful API-41 result in selection-defined deterministic order.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensListing {
    throttle_time_ms: u32,
    tokens: Vec<DelegationToken>,
}

impl DescribeDelegationTokensListing {
    pub(crate) const fn new(throttle_time_ms: u32, tokens: Vec<DelegationToken>) -> Self {
        Self {
            throttle_time_ms,
            tokens,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns complete tokens in deterministic selection order.
    pub fn tokens(&self) -> &[DelegationToken] {
        &self.tokens
    }

    /// Consumes the listing into throttle and unique token owners.
    pub fn into_parts(self) -> (u32, Vec<DelegationToken>) {
        (self.throttle_time_ms, self.tokens)
    }
}
