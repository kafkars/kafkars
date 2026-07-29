//! Stable successful result for one metadata-quorum voter removal.

/// Kafka's acknowledgement of one metadata-quorum voter removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoveRaftVoterResult {
    pub(super) throttle_time_ms: u32,
}

impl RemoveRaftVoterResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Consumes the result into Kafka's throttle observation.
    pub const fn into_parts(self) -> u32 {
        self.throttle_time_ms
    }
}
