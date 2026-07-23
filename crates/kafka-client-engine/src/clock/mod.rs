//! Monotonic epoch mapping and generation-fenced engine timer execution.

mod error;
mod monotonic;
mod timer;

pub(crate) use error::{BatchTimerError, ClockError};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "clock ownership precedes the integrated engine host"
    )
)]
pub(crate) use monotonic::MonotonicClock;
pub(crate) use timer::BatchTimers;

#[cfg(test)]
mod monotonic_test;
#[cfg(test)]
mod timer_test;
