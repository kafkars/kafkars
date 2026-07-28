//! Deterministically ordered user SCRAM descriptions with throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

use super::ScramCredentialInfo;

/// Fully settled per-user SCRAM credential descriptions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsResult {
    throttle_time: Duration,
    users: BatchResult<String, Vec<ScramCredentialInfo>>,
}

impl DescribeUserScramCredentialsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        users: BatchResult<String, Vec<ScramCredentialInfo>>,
    ) -> Self {
        Self {
            throttle_time,
            users,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-user outcomes in the query's deterministic order.
    ///
    /// Explicit filters retain caller order. An all-user query uses user-name
    /// UTF-8 byte order. Each successful value contains only mechanism and
    /// iteration facts; credential secrets never cross this API.
    pub const fn users(&self) -> &BatchResult<String, Vec<ScramCredentialInfo>> {
        &self.users
    }

    /// Consumes the result into deterministically ordered user outcomes.
    pub fn into_users(self) -> BatchResult<String, Vec<ScramCredentialInfo>> {
        self.users
    }
}
