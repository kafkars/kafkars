//! Terminal outcomes and errors emitted by producer-operation transitions.

use core::fmt;

use crate::ByteCount;

/// Certainty attached to a failed producer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// The operation did not cross the transport ownership boundary.
    NotSent,
    /// The operation may have reached Kafka and a blind retry may duplicate it.
    PossiblySent,
}

/// Terminal producer result retained for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerCompletion {
    /// Kafka acknowledged the record batch containing the operation.
    Delivered,
    /// The operation failed with explicit delivery certainty.
    Failed(DeliveryStatus),
}

/// Resource-accounting fact emitted when an operation becomes terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalRelease {
    pub(crate) released_bytes: Option<ByteCount>,
}

impl TerminalRelease {
    /// Returns bytes released from the admitted producer budget, when any.
    pub const fn released_bytes(self) -> Option<ByteCount> {
        self.released_bytes
    }
}

/// Rejected producer-operation state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    /// The requested action does not belong to the current lifecycle stage.
    InvalidState,
    /// The engine reported a different batch from the one awaiting ownership.
    BatchMismatch,
    /// A claimed deadline expiration preceded the operation deadline.
    DeadlineNotElapsed,
    /// The operation already owns a terminal completion.
    AlreadyCompleted,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState => formatter.write_str("invalid producer operation state"),
            Self::BatchMismatch => formatter.write_str("producer batch identity does not match"),
            Self::DeadlineNotElapsed => formatter.write_str("producer deadline has not elapsed"),
            Self::AlreadyCompleted => formatter.write_str("producer operation already completed"),
        }
    }
}

impl std::error::Error for TransitionError {}
