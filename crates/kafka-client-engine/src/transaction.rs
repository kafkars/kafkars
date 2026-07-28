//! Declarative facade for transactional execution ownership.

mod completion;
mod execution;
mod initialization;
mod lifecycle;
pub(crate) mod partition_enrollment;

pub(crate) use execution::TransactionExecutionHost;
pub use initialization::{
    TransactionBeginAccepted, TransactionControlError, TransactionControlErrorKind,
    TransactionEndAccepted, TransactionEndAdmissionError, TransactionEndObserver,
    TransactionEndObserverError, TransactionEndOutcome, TransactionInitializationAccepted,
    TransactionInitializationAcceptedFaultKind, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationCapture,
    TransactionInitializationCaptureError, TransactionInitializationDeliveryStatus,
    TransactionInitializationFailure, TransactionInitializationFailureKind,
    TransactionInitializationObserver, TransactionInitializationObserverError,
    TransactionInitializationOutcome, TransactionInitializationRequest, TransactionToken,
    TransactionalOwnerHandle,
};
pub(crate) use initialization::{
    TransactionInitializationAdmissionPort, TransactionInitializationHost,
    TransactionInitializationHostError, TransactionInitializationShardLockError,
    TransactionInitializationShardOwner, TransactionInitializationTurn,
};
pub(crate) use lifecycle::{TransactionLifecycleHostError, TransactionLifecycleTurn};
