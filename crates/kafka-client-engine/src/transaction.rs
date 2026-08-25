//! Declarative facade for transactional execution ownership.

mod completion;
mod execution;
mod initialization;
mod lifecycle;
mod offset_commit;
pub(crate) mod partition_enrollment;
pub(crate) mod send;

pub(crate) use execution::{
    TransactionExecutionHost, TransactionExecutionSendAdmissionError,
    TransactionExecutionSendAdmissionErrorKind,
};
pub use initialization::{
    TransactionBatchSendAccepted, TransactionBatchSendAdmissionError, TransactionBatchSendMetadata,
    TransactionBatchSendObserver, TransactionBatchSendOutcome, TransactionBeginAccepted,
    TransactionControlError, TransactionControlErrorKind, TransactionEndAccepted,
    TransactionEndAdmissionError, TransactionEndDeliveryStatus, TransactionEndFailure,
    TransactionEndFailureKind, TransactionEndIntent, TransactionEndObserver,
    TransactionEndObserverError, TransactionEndOutcome, TransactionInitializationAccepted,
    TransactionInitializationAcceptedFaultKind, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationCapture,
    TransactionInitializationCaptureError, TransactionInitializationDeliveryStatus,
    TransactionInitializationFailure, TransactionInitializationFailureKind,
    TransactionInitializationObserver, TransactionInitializationObserverError,
    TransactionInitializationOutcome, TransactionInitializationRequest, TransactionOffsetsAccepted,
    TransactionOffsetsAdmissionError, TransactionOffsetsAdmissionErrorKind,
    TransactionOffsetsCapture, TransactionOffsetsConsequence, TransactionOffsetsDeliveryStatus,
    TransactionOffsetsFailure, TransactionOffsetsFailureKind, TransactionOffsetsObserver,
    TransactionOffsetsObserverError, TransactionOffsetsOutcome, TransactionOffsetsStage,
    TransactionSendAccepted, TransactionSendAdmissionError, TransactionSendAdmissionErrorKind,
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailure,
    TransactionSendFailureKind, TransactionSendMetadata, TransactionSendObserver,
    TransactionSendObserverError, TransactionSendOutcome, TransactionToken,
    TransactionalOwnerHandle,
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
