//! One engine epoch mapped into checked nanosecond core ticks.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, Moment};

use super::{ClockError, OperationDeadline};

/// One monotonic observation and deadline captured at the same method boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeadlineCapture {
    now: Moment,
    deadline: OperationDeadline,
}

impl DeadlineCapture {
    /// Returns the boundary observation supplied to deterministic policy.
    pub(crate) const fn now(self) -> Moment {
        self.now
    }

    /// Returns the checked absolute deadline created at that boundary.
    pub(crate) const fn deadline(self) -> Deadline {
        self.deadline.core()
    }

    /// Returns both unchanged absolute representations captured at the boundary.
    pub(crate) const fn operation_deadline(self) -> OperationDeadline {
        self.deadline
    }
}

/// Unique mapping from operating-system monotonic instants to core ticks.
#[derive(Debug)]
pub(crate) struct MonotonicClock {
    epoch: Instant,
}

impl MonotonicClock {
    /// Captures the one epoch shared by this engine host.
    pub(crate) fn new() -> Self {
        Self::from_epoch(Instant::now())
    }

    /// Returns the current monotonic observation without wall-clock access.
    pub(crate) fn now(&self) -> Result<Moment, ClockError> {
        self.moment_at(Instant::now())
    }

    /// Captures one boundary observation and constructs its absolute deadline.
    pub(crate) fn capture_deadline_after(
        &self,
        timeout: Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        let boundary = Instant::now();
        self.capture_deadline_at(boundary, timeout)
    }

    pub(super) const fn from_epoch(epoch: Instant) -> Self {
        Self { epoch }
    }

    pub(super) fn moment_at(&self, instant: Instant) -> Result<Moment, ClockError> {
        let elapsed = instant
            .checked_duration_since(self.epoch)
            .ok_or(ClockError::BeforeEpoch)?;
        Ok(Moment::from_tick(duration_ticks(elapsed)?))
    }

    pub(super) fn capture_deadline_at(
        &self,
        boundary: Instant,
        timeout: Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        let absolute_instant = absolute_instant_after(boundary, timeout)?;
        let now = self.moment_at(boundary)?;
        let deadline = deadline_after_moment(now, timeout)?;
        Ok(DeadlineCapture {
            now,
            deadline: OperationDeadline::from_boundary_parts(deadline, absolute_instant),
        })
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) fn duration_ticks(duration: Duration) -> Result<u64, ClockError> {
    u64::try_from(duration.as_nanos()).map_err(|_overflow| ClockError::TickOverflow)
}

pub(super) fn deadline_after_moment(
    now: Moment,
    timeout: Duration,
) -> Result<Deadline, ClockError> {
    let ticks = duration_ticks(timeout)?;
    now.checked_deadline_after(ticks)
        .ok_or(ClockError::DeadlineOverflow)
}

pub(super) fn absolute_instant_after(
    boundary: Instant,
    timeout: Duration,
) -> Result<Instant, ClockError> {
    boundary
        .checked_add(timeout)
        .ok_or(ClockError::InstantOverflow)
}
