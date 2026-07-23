//! Errors rejected by deterministic producer-machine transitions.

use core::fmt;

use crate::{AdmissionRejection, CapacityError, CompletionLedgerError, TransitionError};

/// Rejected producer-machine transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerMachineError {
    /// Admission policy rejected otherwise valid explicit record facts.
    Admission(AdmissionRejection),
    /// The operation is not retained by this producer.
    UnknownOperation,
    /// The requested lifecycle transition is invalid.
    Transition(TransitionError),
    /// Terminal-completion ownership rejected the transition.
    Completion(CompletionLedgerError),
    /// Retained-byte accounting rejected the transition.
    Capacity(CapacityError),
}

impl fmt::Display for ProducerMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(reason) => write!(formatter, "producer admission rejected: {reason:?}"),
            Self::UnknownOperation => formatter.write_str("producer operation is unknown"),
            Self::Transition(error) => error.fmt(formatter),
            Self::Completion(error) => error.fmt(formatter),
            Self::Capacity(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerMachineError {}
