//! Inert producer-fencing intent with one submission boundary.

use std::time::{Duration, Instant};

use crate::bridge::{
    admin::AdminEngine,
    fence_producers::{AdminFenceProducers, FenceProducersAdminRequest},
};

use super::FenceProducers;

/// Inert caller-ordered transactional producer-fencing request.
#[must_use = "call submit to admit the FenceProducers operation"]
pub struct FenceProducersBuilder {
    engine: AdminEngine,
    request: FenceProducersAdminRequest,
    timeout: SubmissionTimeout,
}

impl FenceProducersBuilder {
    pub(crate) fn new(
        engine: AdminEngine,
        request: FenceProducersAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout: SubmissionTimeout::new(timeout),
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = self.timeout.with_timeout(timeout);
        self
    }

    /// Attempts bounded admission and returns one named terminal observer.
    ///
    /// This call is the public operation boundary. Its absolute deadline is
    /// captured before public result preparation or engine admission.
    pub fn submit(self) -> FenceProducers {
        let deadline = match self.timeout.capture() {
            Ok(deadline) => deadline,
            Err(CallDeadlineError::Elapsed) => {
                return FenceProducers::from_bridge(AdminFenceProducers::deadline_elapsed());
            }
            Err(CallDeadlineError::Overflow) => {
                return FenceProducers::from_bridge(AdminFenceProducers::invalid_deadline());
            }
        };
        FenceProducers::from_bridge(self.engine.submit_fence_producers(self.request, deadline))
    }
}

impl std::fmt::Debug for FenceProducersBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FenceProducersBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout.timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SubmissionTimeout {
    timeout: Duration,
}

impl SubmissionTimeout {
    pub(super) const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub(super) const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn capture(self) -> Result<Instant, CallDeadlineError> {
        self.capture_at(Instant::now())
    }

    pub(super) fn capture_at(self, boundary: Instant) -> Result<Instant, CallDeadlineError> {
        let deadline = boundary
            .checked_add(self.timeout)
            .ok_or(CallDeadlineError::Overflow)?;
        if self.timeout.is_zero() {
            Err(CallDeadlineError::Elapsed)
        } else {
            Ok(deadline)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CallDeadlineError {
    Elapsed,
    Overflow,
}
