//! First-occurrence user SCRAM alteration outcomes with throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

/// Fully settled per-user SCRAM credential alterations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsResult {
    throttle_time: Duration,
    users: BatchResult<String, ()>,
}

impl AlterUserScramCredentialsResult {
    pub(crate) const fn new(throttle_time: Duration, users: BatchResult<String, ()>) -> Self {
        Self {
            throttle_time,
            users,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns outcomes in affected-user first-occurrence order.
    pub const fn users(&self) -> &BatchResult<String, ()> {
        &self.users
    }

    /// Consumes the result into affected-user first-occurrence order.
    pub fn into_users(self) -> BatchResult<String, ()> {
        self.users
    }
}
