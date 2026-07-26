//! Closed lifecycle stages for one transaction initialization.

/// Current ownership stage for one initialization attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationState {
    /// Accepted after the engine reserved terminal capacity.
    Ready,
    /// The unchanged request awaits driver admission.
    AwaitingDriver,
    /// The driver owns the sole request attempt.
    Submitted,
    /// Core assigned the sole terminal outcome.
    Completed,
}
