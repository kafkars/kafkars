//! Deterministic producer, consumer, transaction, and admin policy.

#![forbid(unsafe_code)]

mod admission;
mod capacity;
mod completion;
mod operation;
mod operation_outcome;
mod producer;
mod producer_effect;
mod producer_error;
mod producer_input;
mod producer_record;
mod producer_transition;
mod types;

pub use admission::{AdmissionRejection, Admitted, TryAdmitError};
pub use capacity::{ByteBudget, CapacityError};
pub use completion::{CompletionLedger, CompletionLedgerError};
pub use operation::{ProducerOperation, ProducerOperationState};
pub use operation_outcome::{DeliveryStatus, ProducerCompletion, TerminalRelease, TransitionError};
pub use producer::ProducerMachine;
pub use producer_effect::{
    AcknowledgementPolicy, CompressionPolicy, ProducerEffect, ProducerTransition,
};
pub use producer_error::ProducerMachineError;
pub use producer_input::ProducerInput;
pub use producer_record::{BatchId, ExplicitRecord, PartitionIndex, PayloadId, TopicId};
pub use types::{ByteCount, Deadline, Moment, OperationId};

#[cfg(test)]
mod capacity_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod producer_reclaim_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod producer_transition_test;
