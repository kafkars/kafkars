//! Declarative facade for one concrete Admin partition transaction abort.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{
    AbortPartitionTransactionAdmissionError, AbortPartitionTransactionAdmissionErrorKind,
};
pub use handle::{AbortPartitionTransactionAccepted, AbortPartitionTransactionAcceptedFaultKind};
pub use model::AbortPartitionTransactionRequest;
pub use observer::AbortPartitionTransactionObserver;
pub use outcome::{
    AbortPartitionTransactionBrokerError, AbortPartitionTransactionDeliveryStatus,
    AbortPartitionTransactionFailure, AbortPartitionTransactionFailureKind,
    AbortPartitionTransactionObserverError, AbortPartitionTransactionOutcome,
};

pub(crate) use error::AbortPartitionTransactionHostError;
pub(crate) use host::{
    ABORT_PARTITION_TRANSACTION_CAPACITY, AbortPartitionTransactionHost,
    AbortPartitionTransactionTurn,
};
pub(crate) use shard::{
    AbortPartitionTransactionAdmissionPort, AbortPartitionTransactionShardLockError,
    AbortPartitionTransactionShardOwner, AbortPartitionTransactionShardWake,
    AbortPartitionTransactionShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
