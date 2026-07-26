//! Fence-bound deadline ownership for one engine-initiated Fetch attempt.

use std::time::Duration;

use kafka_client_core::FetchFence;

use crate::clock::{ClockError, MonotonicClock, OperationDeadline};

/// One freshly captured internal-attempt deadline bound to an exact core effect.
#[must_use = "a Fetch attempt deadline must be bound to its exact prepared effect"]
pub(crate) struct FetchAttemptDeadline {
    fence: FetchFence,
    operation: OperationDeadline,
}

impl FetchAttemptDeadline {
    /// Freshly captures and binds the one boundary for this exact Fetch.
    pub(crate) fn capture_for_fetch(
        fence: FetchFence,
        clock: &MonotonicClock,
        timeout: Duration,
    ) -> Result<Self, ClockError> {
        let capture = clock.capture_deadline_after(timeout)?;
        Ok(Self {
            fence,
            operation: capture.operation_deadline(),
        })
    }

    pub(crate) const fn fence(&self) -> FetchFence {
        self.fence
    }

    #[cfg(test)]
    pub(crate) const fn operation(&self) -> OperationDeadline {
        self.operation
    }

    pub(super) const fn into_operation(self) -> OperationDeadline {
        self.operation
    }

    #[cfg(test)]
    pub(crate) const fn from_parts_for_test(
        fence: FetchFence,
        operation: OperationDeadline,
    ) -> Self {
        Self { fence, operation }
    }

    #[cfg(test)]
    pub(crate) const fn into_parts_for_test(self) -> (FetchFence, OperationDeadline) {
        (self.fence, self.operation)
    }
}
