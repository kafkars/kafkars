//! Checked monotonic-clock conversion failures without saturation.

use core::fmt;

/// Failure to represent a monotonic observation or absolute deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClockError {
    /// The supplied instant precedes the engine's shared monotonic epoch.
    BeforeEpoch,
    /// Nanoseconds since the shared epoch exceed the core tick domain.
    TickOverflow,
    /// The operating-system monotonic instant cannot represent the deadline.
    InstantOverflow,
    /// Adding the requested timeout exceeds the core deadline domain.
    DeadlineOverflow,
}

impl fmt::Display for ClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeEpoch => formatter.write_str("instant precedes monotonic clock epoch"),
            Self::TickOverflow => formatter.write_str("monotonic nanosecond tick overflow"),
            Self::InstantOverflow => formatter.write_str("absolute monotonic instant overflow"),
            Self::DeadlineOverflow => formatter.write_str("absolute deadline overflow"),
        }
    }
}

impl std::error::Error for ClockError {}

/// Failure to retain another active producer batch timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BatchTimerError {
    capacity: usize,
}

impl BatchTimerError {
    pub(super) const fn capacity(capacity: usize) -> Self {
        Self { capacity }
    }

    /// Returns the configured active-timer limit.
    pub(crate) const fn limit(self) -> usize {
        self.capacity
    }
}

impl fmt::Display for BatchTimerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "producer batch timer capacity {} is full",
            self.capacity
        )
    }
}

impl std::error::Error for BatchTimerError {}
