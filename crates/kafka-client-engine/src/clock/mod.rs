//! Monotonic epoch mapping and generation-fenced engine timer execution.

mod error;
mod monotonic;
mod operation_deadline;
mod timer;

pub(crate) use error::{BatchTimerError, ClockError};
pub(crate) use monotonic::{DeadlineCapture, MonotonicClock};
pub(crate) use operation_deadline::OperationDeadline;
pub(crate) use timer::BatchTimers;

#[cfg(test)]
mod monotonic_test;
#[cfg(test)]
mod operation_deadline_test;
#[cfg(test)]
mod timer_test;
