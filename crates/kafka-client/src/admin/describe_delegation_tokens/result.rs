//! Deterministically ordered delegation tokens with throttle observation.

use std::time::Duration;

use super::DelegationToken;

/// Fully settled token descriptions with unique secret ownership.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensResult {
    throttle_time: Duration,
    tokens: Vec<DelegationToken>,
}

impl DescribeDelegationTokensResult {
    pub(crate) const fn new(throttle_time: Duration, tokens: Vec<DelegationToken>) -> Self {
        Self {
            throttle_time,
            tokens,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns complete token facts in deterministic order.
    ///
    /// An owner-filtered query retains caller owner order and token-ID byte
    /// order within each owner. An all-visible query uses principal type,
    /// principal name, then token-ID byte order.
    pub fn tokens(&self) -> &[DelegationToken] {
        &self.tokens
    }

    /// Consumes the result into Kafka's throttle and complete token facts.
    pub fn into_parts(self) -> (Duration, Vec<DelegationToken>) {
        (self.throttle_time, self.tokens)
    }

    /// Consumes the result into complete token facts and unique HMAC owners.
    pub fn into_tokens(self) -> Vec<DelegationToken> {
        self.tokens
    }
}
