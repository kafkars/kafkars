//! Declarative facade for one private transactional-owner initialization.

mod error;
mod host;
mod model;
mod observer;
mod outcome;
mod port;
mod retained_owner;
mod shard;

pub(crate) use error::{
    TransactionInitializationAdmissionError, TransactionInitializationAdmissionErrorKind,
    TransactionInitializationHostError,
};
pub(crate) use host::{TransactionInitializationHost, TransactionInitializationTurn};
pub(crate) use model::TransactionInitializationRequest;
pub(crate) use observer::TransactionInitializationObserver;
pub(crate) use outcome::{
    TransactionInitializationAccepted, TransactionInitializationOutcome, TransactionalOwnerHandle,
};
#[cfg(test)]
pub(crate) use outcome::{
    TransactionInitializationDeliveryStatus, TransactionInitializationFailureKind,
};
pub(crate) use port::TransactionInitializationAdmissionPort;
use retained_owner::RetainedTransactionInitializationOutcome;
pub(crate) use shard::{
    TransactionInitializationShardLockError, TransactionInitializationShardOwner,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod retained_owner_test;
