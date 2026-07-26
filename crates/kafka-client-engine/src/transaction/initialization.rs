//! Declarative facade for one private transactional-owner initialization.

mod capture;
mod error;
mod host;
mod model;
mod observer;
mod outcome;
mod owner;
mod port;
mod retained_owner;
mod shard;

pub use capture::TransactionInitializationCapture;
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
pub(crate) use port::TransactionInitializationAdmissionPort;
use retained_owner::RetainedTransactionInitializationOutcome;
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
mod owner_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod retained_owner_test;
