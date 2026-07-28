//! Scalar-only Admin `ListOffsets` response facts without generated ownership.

use kafka_client_core::AdminListOffsetOutcome;

/// One correlated partition response retained for deterministic interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAdminListOffsetsResponse {
    throttle_time_ms: u32,
    outcome: AdminListOffsetOutcome,
}

impl NormalizedAdminListOffsetsResponse {
    pub(super) const fn new(throttle_time_ms: u32, outcome: AdminListOffsetOutcome) -> Self {
        Self {
            throttle_time_ms,
            outcome,
        }
    }

    /// Returns Kafka's nonnegative broker throttle observation.
    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the correlated partition result.
    #[cfg(test)]
    pub(crate) const fn outcome(&self) -> &AdminListOffsetOutcome {
        &self.outcome
    }

    /// Consumes the response into deterministic core input.
    pub(crate) fn into_parts(self) -> (u32, AdminListOffsetOutcome) {
        (self.throttle_time_ms, self.outcome)
    }
}
