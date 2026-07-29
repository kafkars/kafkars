//! Declarative facade for the concrete Admin `DescribeTransactions` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;
mod value;

pub use error::{
    AdminDescribeTransactionsAdmissionError, AdminDescribeTransactionsAdmissionErrorKind,
};
pub use handle::{AdminDescribeTransactionsAccepted, AdminDescribeTransactionsAcceptedFaultKind};
pub use model::AdminDescribeTransactionsRequest;
pub use observer::AdminDescribeTransactionsObserver;
pub use outcome::{
    AdminDescribeTransactionEngineBrokerError, AdminDescribeTransactionEngineResult,
    AdminDescribeTransactionsDeliveryStatus, AdminDescribeTransactionsEngineBatch,
    AdminDescribeTransactionsFailure, AdminDescribeTransactionsFailureKind,
    AdminDescribeTransactionsObserverError, AdminDescribeTransactionsOutcome,
};
pub use value::{AdminDescribeTransactionDescription, AdminDescribeTransactionTopic};

pub(crate) use error::AdminDescribeTransactionsHostError;
pub(crate) use host::{
    ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY, AdminDescribeTransactionsHost,
    AdminDescribeTransactionsTurn,
};
pub(crate) use shard::{
    AdminDescribeTransactionsAdmissionPort, AdminDescribeTransactionsShardLockError,
    AdminDescribeTransactionsShardOwner, AdminDescribeTransactionsShardWake,
    AdminDescribeTransactionsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
