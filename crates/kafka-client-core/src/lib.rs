//! Deterministic producer, consumer, transaction, and admin policy.

#![forbid(unsafe_code)]

mod admission;
mod capacity;
mod completion;
mod operation;
mod producer;
mod types;

pub use admission::{AdmissionRejection, Admitted, TryAdmitError};
pub use capacity::{ByteBudget, CapacityError};
pub use completion::{CompletionLedger, CompletionLedgerError};
pub use operation::{
    DeliveryStatus, ProducerCompletion, ProducerOperation, ProducerOperationState, TerminalEffects,
    TransitionError,
};
pub use producer::{ProducerMachine, ProducerMachineError};
pub use types::{ByteCount, Deadline, OperationId};

#[cfg(test)]
mod capacity_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod producer_test;
