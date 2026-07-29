//! Stable successful result for one broker unregistration.

/// Kafka's acknowledgement of a broker unregistration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerResult {
    pub(super) throttle_time_ms: u32,
}

impl UnregisterBrokerResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Consumes the result into Kafka's throttle observation.
    pub const fn into_parts(self) -> u32 {
        self.throttle_time_ms
    }
}
