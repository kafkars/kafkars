//! Deterministic cluster-wide transaction-listing policy.

mod failure;
mod machine;
mod model;
mod normalization;
mod outcome;
mod transition;
mod value;

pub use failure::{AdminListTransactionsFailure, AdminListTransactionsFailureKind};
pub use machine::{
    AdminListTransactionsEffect, AdminListTransactionsInput, AdminListTransactionsMachine,
    AdminListTransactionsMachineError, AdminListTransactionsState, AdminListTransactionsTransition,
};
pub use model::{
    AdminListTransactionsPlan, AdminListTransactionsPlanError,
    LIST_TRANSACTIONS_MAX_FILTER_STATE_BYTES, LIST_TRANSACTIONS_MAX_PRODUCER_ID_FILTERS,
    LIST_TRANSACTIONS_MAX_STATE_FILTERS, LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_PATTERN_BYTES,
};
pub use outcome::{
    AdminListTransactionsBatch, AdminListTransactionsBrokerError,
    AdminListTransactionsBrokerOutcome, AdminListTransactionsTerminal,
};
pub use value::{
    AdminListedTransaction, LIST_TRANSACTIONS_MAX_BROKERS,
    LIST_TRANSACTIONS_MAX_RESULT_STRING_BYTES, LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES,
    LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES, LIST_TRANSACTIONS_MAX_TRANSACTIONS,
    LIST_TRANSACTIONS_MAX_UNKNOWN_STATE_FILTERS,
};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod transition_test;
#[cfg(test)]
mod transition_timeout_test;
#[cfg(test)]
mod value_test;
