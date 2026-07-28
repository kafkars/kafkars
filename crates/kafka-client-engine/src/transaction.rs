//! Declarative facade for transactional execution ownership.

mod completion;
mod execution;
mod initialization;
mod lifecycle;
pub(crate) mod partition_enrollment;
pub(crate) mod send;

pub(crate) use execution::{
    TransactionExecutionHost, TransactionExecutionSendAdmissionError,
    TransactionExecutionSendAdmissionErrorKind,
};
pub use initialization::{
    TransactionBeginAccepted, TransactionControlError, TransactionControlErrorKind,
    TransactionEndAccepted, TransactionEndAdmissionError, TransactionEndObserver,
    TransactionEndObserverError, TransactionEndOutcome, TransactionInitializationAccepted,
    TransactionInitializationAcceptedFaultKind, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationCapture,
    TransactionInitializationCaptureError, TransactionInitializationDeliveryStatus,
    TransactionInitializationFailure, TransactionInitializationFailureKind,
    TransactionInitializationObserver, TransactionInitializationObserverError,
    TransactionInitializationOutcome, TransactionInitializationRequest, TransactionSendAccepted,
    TransactionSendAdmissionError, TransactionSendAdmissionErrorKind, TransactionSendConsequence,
    TransactionSendDeliveryStatus, TransactionSendFailure, TransactionSendFailureKind,
    TransactionSendMetadata, TransactionSendObserver, TransactionSendObserverError,
    TransactionSendOutcome, TransactionToken, TransactionalOwnerHandle,
};
pub(crate) use initialization::{
    TransactionInitializationAdmissionPort, TransactionInitializationHost,
    TransactionInitializationHostError, TransactionInitializationShardLockError,
    TransactionInitializationShardOwner, TransactionInitializationTurn,
};
pub(in crate::transaction) use lifecycle::TransactionSendReplacement;
pub(crate) use lifecycle::{
    TransactionExecutionLimits, TransactionLifecycleHost, TransactionLifecycleHostError,
    TransactionLifecycleTurn,
};
