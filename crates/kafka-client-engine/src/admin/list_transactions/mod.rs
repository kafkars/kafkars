//! Declarative facade for the concrete Admin `ListTransactions` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{AdminListTransactionsAdmissionError, AdminListTransactionsAdmissionErrorKind};
pub use handle::{AdminListTransactionsAccepted, AdminListTransactionsAcceptedFaultKind};
pub use model::AdminListTransactionsRequest;
pub use observer::AdminListTransactionsObserver;
pub use outcome::{
    AdminListTransactionsBrokerError, AdminListTransactionsDeliveryStatus,
    AdminListTransactionsDiscoveryError, AdminListTransactionsEngineBatch,
    AdminListTransactionsFailure, AdminListTransactionsFailureKind,
    AdminListTransactionsObserverError, AdminListTransactionsOutcome, AdminListedTransaction,
};

pub(crate) use error::AdminListTransactionsHostError;
pub(crate) use host::{
    ADMIN_LIST_TRANSACTIONS_CAPACITY, AdminListTransactionsHost,
    AdminListTransactionsSubmissionKind, AdminListTransactionsTurn,
};
pub(crate) use shard::{
    AdminListTransactionsAdmissionPort, AdminListTransactionsShardLockError,
    AdminListTransactionsShardOwner, AdminListTransactionsShardWake,
    AdminListTransactionsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
