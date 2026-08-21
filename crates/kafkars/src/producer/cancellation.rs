//! Public stage-aware producer cancellation outcome.

/// Immediate result of asking the engine to cancel one accepted delivery.
///
/// Cancellation never consumes the delivery observer. The same [`super::Delivery`]
/// still resolves to its one authoritative terminal delivery result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationOutcome {
    /// The operation was cancelled before transport ownership.
    CancelledNotSent,
    /// Transport may already own the operation, so cancellation cannot be promised.
    TooLate,
    /// The operation had already selected a terminal result, which remains on the delivery.
    AlreadyTerminal,
}
