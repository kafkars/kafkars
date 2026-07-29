//! Successful token creation and Kafka throttle observation.

use std::time::Duration;

use super::DelegationToken;

/// Fully settled result for one created delegation token.
#[derive(Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenResult {
    throttle_time: Duration,
    token: DelegationToken,
}

impl CreateDelegationTokenResult {
    pub(crate) const fn new(throttle_time: Duration, token: DelegationToken) -> Self {
        Self {
            throttle_time,
            token,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the uniquely owned created token.
    pub const fn token(&self) -> &DelegationToken {
        &self.token
    }

    /// Consumes the result into Kafka's throttle and the unique token.
    pub fn into_parts(self) -> (Duration, DelegationToken) {
        (self.throttle_time, self.token)
    }

    /// Consumes the result into the unique created token.
    pub fn into_token(self) -> DelegationToken {
        self.token
    }
}
