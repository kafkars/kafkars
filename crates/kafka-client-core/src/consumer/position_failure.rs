//! Lossless semantic failures for one fenced position-resolution attempt.

use core::num::NonZeroI16;

/// One engine-observed terminal fact from a concrete position lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionResolutionAttemptFailure {
    /// The exact tracked call reached its original absolute deadline.
    DeadlineElapsed,
    /// The driver rejected permanent ownership of the request.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed `ListOffsets` error code.
    Broker(NonZeroI16),
    /// The selected API version cannot preserve requested semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The generated or decoded response exceeded a configured bound.
    ResponseTooLarge,
}

/// Core-owned terminal reason one position resolution cannot become fetch-ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionResolutionFailure {
    /// The original public resolution deadline elapsed.
    DeadlineElapsed,
    /// One concrete attempt terminated before the core-owned deadline elapsed.
    Attempt(PositionResolutionAttemptFailure),
    /// The positive throttle duration could not become an absolute deadline.
    ThrottleDeadlineOverflow,
}
