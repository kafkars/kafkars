//! Producer admission rejections returned before ownership transfer.

/// Why immediate producer admission returned caller ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Producer admission has closed permanently.
    Closed,
    /// The public operation deadline had already elapsed.
    DeadlineElapsed,
    /// Retaining the record would exceed the producer byte budget.
    ByteCapacity,
    /// No terminal-completion slot is available.
    CompletionCapacity,
    /// The current partition batch reached its count limit before accumulation caught up.
    AccumulatorPending,
    /// Retained-byte arithmetic could not represent the requested reservation.
    ByteCountOverflow,
    /// The producer exhausted its monotonic operation identity space.
    IdentityExhausted,
    /// The producer exhausted its monotonic batch identity space.
    BatchIdentityExhausted,
    /// Linger deadline construction exceeded the monotonic time domain.
    DeadlineOverflow,
}
