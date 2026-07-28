//! Declarative public admission and observation boundary for group checkpoints.

mod admission;
mod observer;
mod outcome;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod test_support;

pub use admission::{
    GroupConsumerCommitAccepted, GroupConsumerCommitAdmissionError,
    GroupConsumerCommitAdmissionErrorKind,
};
pub use observer::{GroupConsumerCommitObserver, GroupConsumerCommitObserverError};
pub use outcome::{
    GroupConsumerCommitBatch, GroupConsumerCommitBrokerError, GroupConsumerCommitDeliveryStatus,
    GroupConsumerCommitFailure, GroupConsumerCommitFailureKind, GroupConsumerCommitOutcome,
    GroupConsumerCommitPartitionOutcome, GroupConsumerCommitPartitionResult,
};
