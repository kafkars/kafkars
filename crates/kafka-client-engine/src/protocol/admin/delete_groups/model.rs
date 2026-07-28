//! Scalar-only Admin `DeleteConsumerGroups` response facts without generated ownership.

use kafka_client_core::DeleteConsumerGroupsOutcome;

/// One correlated group response retained for deterministic interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDeleteConsumerGroupsResponse {
    throttle_time_ms: u32,
    outcome: DeleteConsumerGroupsOutcome,
    retained_bytes: usize,
}

impl NormalizedDeleteConsumerGroupsResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        outcome: DeleteConsumerGroupsOutcome,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcome,
            retained_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn outcome(&self) -> &DeleteConsumerGroupsOutcome {
        &self.outcome
    }

    pub(crate) fn into_parts(self) -> (u32, DeleteConsumerGroupsOutcome, usize) {
        (self.throttle_time_ms, self.outcome, self.retained_bytes)
    }
}
