//! Stable successful result for one committed voter addition.

/// Kafka's acknowledgement that the voter set was committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddRaftVoterResult {
    pub(super) throttle_time_ms: u32,
}

impl AddRaftVoterResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Consumes the result into Kafka's throttle observation.
    pub const fn into_parts(self) -> u32 {
        self.throttle_time_ms
    }
}
