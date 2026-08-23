//! Declarative facade for public share-acknowledgement admission and observation.

mod admission;
#[cfg(test)]
mod admission_test;
mod completion;
mod observer;
mod outcome;
mod translation;

pub use admission::{
    ShareAcknowledgementAccepted, ShareAcknowledgementAdmissionError,
    ShareAcknowledgementAdmissionErrorKind,
};
pub use observer::{ShareAcknowledgementObserver, ShareAcknowledgementObserverError};
pub use outcome::{
    ShareAcknowledgeBrokerError, ShareAcknowledgeDeliveryStatus, ShareAcknowledgeFailure,
    ShareAcknowledgeFailureKind, ShareAcknowledgeOutcome, ShareAcknowledgePartitionOutcome,
    ShareAcknowledgeResponse,
};

pub(in crate::consumer) use completion::ShareAcknowledgementCompletionOwner;
pub(in crate::consumer) use translation::public_outcome;
