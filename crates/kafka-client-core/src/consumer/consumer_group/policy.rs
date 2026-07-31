//! Immutable local attempt bound for broker-paced KIP-848 heartbeats.

/// Local timeout applied to each steady-state heartbeat attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatPolicy {
    attempt_timeout_ticks: u64,
}

impl ConsumerGroupHeartbeatPolicy {
    /// Creates a positive heartbeat-attempt timeout.
    pub const fn try_new(
        attempt_timeout_ticks: u64,
    ) -> Result<Self, ConsumerGroupHeartbeatPolicyError> {
        if attempt_timeout_ticks == 0 {
            Err(ConsumerGroupHeartbeatPolicyError::ZeroAttemptTimeout)
        } else {
            Ok(Self {
                attempt_timeout_ticks,
            })
        }
    }

    /// Returns the local attempt timeout in deterministic ticks.
    pub const fn attempt_timeout_ticks(self) -> u64 {
        self.attempt_timeout_ticks
    }
}

/// Invalid KIP-848 local heartbeat policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatPolicyError {
    /// A zero timeout cannot bound an admitted attempt.
    ZeroAttemptTimeout,
}
