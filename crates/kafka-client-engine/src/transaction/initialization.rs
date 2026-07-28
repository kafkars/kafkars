//! Declarative facade for one private transactional-owner initialization.

mod capture;
mod control;
mod control_error;
mod control_error_mapping;
mod end_observer;
mod error;
mod host;
mod model;
mod observer;
mod outcome;
mod owner;
mod owner_control;
mod owner_parts;
mod owner_send;
mod port;
mod retained_owner;
mod send_admission;
mod send_failure_mapping;
mod send_observer;
mod send_outcome;
mod shard;

pub use capture::TransactionInitializationCapture;
pub(crate) use control::{
    TransactionLifecycleControlAccepted, TransactionLifecycleControlError,
    TransactionLifecycleControlPort, TransactionOwnerLossSignal, TransactionSendControlError,
    TransactionSendControlErrorKind,
};
pub use control_error::{
    TransactionControlError, TransactionControlErrorKind, TransactionEndAdmissionError,
};
pub use end_observer::{
    TransactionEndObserver, TransactionEndObserverError, TransactionEndOutcome,
};
pub(crate) use error::TransactionInitializationHostError;
pub use error::{
    TransactionInitializationAdmissionError, TransactionInitializationAdmissionErrorKind,
    TransactionInitializationCaptureError,
};
pub(crate) use host::{TransactionInitializationHost, TransactionInitializationTurn};
pub use model::TransactionInitializationRequest;
pub use observer::TransactionInitializationObserver;
pub use outcome::{
    TransactionInitializationAccepted, TransactionInitializationAcceptedFaultKind,
    TransactionInitializationDeliveryStatus, TransactionInitializationFailure,
    TransactionInitializationFailureKind, TransactionInitializationObserverError,
    TransactionInitializationOutcome,
};
pub use owner::TransactionalOwnerHandle;
pub use owner_control::{TransactionBeginAccepted, TransactionEndAccepted, TransactionToken};
pub(in crate::transaction) use owner_parts::TransactionalOwnerParts;
pub use owner_send::TransactionSendAccepted;
pub(crate) use port::TransactionInitializationAdmissionPort;
pub(super) use retained_owner::RetainedTransactionInitializationOutcome;
pub use send_admission::{TransactionSendAdmissionError, TransactionSendAdmissionErrorKind};
pub use send_observer::{TransactionSendObserver, TransactionSendObserverError};
pub use send_outcome::{
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailure,
    TransactionSendFailureKind, TransactionSendMetadata, TransactionSendOutcome,
};
pub(crate) use shard::{
    TransactionInitializationShardLockError, TransactionInitializationShardOwner,
};

#[cfg(test)]
mod capture_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod owner_control_test;
#[cfg(test)]
mod owner_send_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod retained_owner_test;
#[cfg(test)]
mod send_outcome_test;
