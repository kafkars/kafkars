//! Stable successful metadata-quorum voter-addition result.

use std::time::Duration;

/// Successful addition of one Kafka metadata-quorum voter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddRaftVoterResult {
    throttle_time: Duration,
}

impl AddRaftVoterResult {
    pub(crate) const fn new(throttle_time: Duration) -> Self {
        Self { throttle_time }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Consumes the result into Kafka's throttle observation.
    pub const fn into_throttle_time(self) -> Duration {
        self.throttle_time
    }
}
