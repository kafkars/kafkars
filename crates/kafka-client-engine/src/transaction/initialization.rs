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
mod port;
mod retained_owner;
mod shard;

pub use capture::TransactionInitializationCapture;
pub(crate) use control::{
    TransactionLifecycleControlAccepted, TransactionLifecycleControlError,
    TransactionLifecycleControlPort, TransactionOwnerLossSignal,
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
pub(crate) use port::TransactionInitializationAdmissionPort;
pub(super) use retained_owner::RetainedTransactionInitializationOutcome;
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
mod owner_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod retained_owner_test;
