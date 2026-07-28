//! Scalar-only Admin `DeleteRecords` response facts without generated ownership.

use kafka_client_core::DeleteRecordsOutcome;

/// One correlated partition response retained for deterministic interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDeleteRecordsResponse {
    throttle_time_ms: u32,
    outcome: DeleteRecordsOutcome,
}

impl NormalizedDeleteRecordsResponse {
    pub(super) const fn new(throttle_time_ms: u32, outcome: DeleteRecordsOutcome) -> Self {
        Self {
            throttle_time_ms,
            outcome,
        }
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn outcome(&self) -> &DeleteRecordsOutcome {
        &self.outcome
    }

    pub(crate) fn into_parts(self) -> (u32, DeleteRecordsOutcome) {
        (self.throttle_time_ms, self.outcome)
    }
}
