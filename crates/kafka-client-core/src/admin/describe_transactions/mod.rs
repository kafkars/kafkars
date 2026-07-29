//! Deterministic policy for caller-selected transaction description.

mod failure;
mod machine;
mod model;
mod normalization;
mod outcome;
mod transition;
mod value;

pub use failure::{AdminDescribeTransactionsFailure, AdminDescribeTransactionsFailureKind};
pub use machine::{
    AdminDescribeTransactionsEffect, AdminDescribeTransactionsInput,
    AdminDescribeTransactionsMachine, AdminDescribeTransactionsMachineError,
    AdminDescribeTransactionsState, AdminDescribeTransactionsTransition,
};
pub use model::{
    AdminDescribeTransactionsPlan, AdminDescribeTransactionsPlanError,
    DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES, DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_IDS,
};
pub use outcome::{
    AdminDescribeTransactionBrokerError, AdminDescribeTransactionOutcome,
    AdminDescribeTransactionResult, AdminDescribeTransactionsBatch,
    AdminDescribeTransactionsTerminal,
};
pub use value::{
    AdminDescribeTransactionDescription, AdminDescribeTransactionTopic,
    DESCRIBE_TRANSACTIONS_MAX_PARTITIONS, DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
    DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES, DESCRIBE_TRANSACTIONS_MAX_TOPICS,
};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
#[cfg(test)]
mod value_test;
