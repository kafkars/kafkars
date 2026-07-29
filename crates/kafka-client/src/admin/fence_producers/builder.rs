//! Inert producer-fencing intent with call-boundary timeout ownership.

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
    deadline: CallDeadline,
}

impl FenceProducersBuilder {
    pub(crate) fn new(
        engine: AdminEngine,
        request: FenceProducersAdminRequest,
        timeout: Duration,
        boundary: Instant,
    ) -> Self {
        Self {
            engine,
            request,
            deadline: CallDeadline::from_boundary(boundary, timeout),
        }
    }

    /// Replaces the timeout while retaining the original Admin call boundary.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.deadline = self.deadline.with_timeout(timeout);
        self
    }

    /// Attempts bounded admission and returns one named terminal observer.
    pub fn submit(self) -> FenceProducers {
        let deadline = match self.deadline.deadline() {
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
            .field("timeout", &self.deadline.timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CallDeadline {
    boundary: Instant,
    timeout: Duration,
}

impl CallDeadline {
    pub(super) const fn from_boundary(boundary: Instant, timeout: Duration) -> Self {
        Self { boundary, timeout }
    }

    pub(super) const fn with_timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }

    fn deadline(self) -> Result<Instant, CallDeadlineError> {
        self.deadline_at(Instant::now())
    }

    fn deadline_at(self, now: Instant) -> Result<Instant, CallDeadlineError> {
        let deadline = self
            .boundary
            .checked_add(self.timeout)
            .ok_or(CallDeadlineError::Overflow)?;
        if deadline.saturating_duration_since(now).is_zero() {
            Err(CallDeadlineError::Elapsed)
        } else {
            Ok(deadline)
        }
    }

    #[cfg(test)]
    pub(super) fn remaining_at(self, now: Instant) -> Result<Duration, CallDeadlineError> {
        self.deadline_at(now)
            .map(|deadline| deadline.saturating_duration_since(now))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CallDeadlineError {
    Elapsed,
    Overflow,
}
