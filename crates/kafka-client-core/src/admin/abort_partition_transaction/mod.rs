//! Deterministic policy for one destructive Admin partition-transaction abort.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AbortPartitionTransactionEffect, AbortPartitionTransactionInput,
    AbortPartitionTransactionMachine, AbortPartitionTransactionMachineError,
    AbortPartitionTransactionState, AbortPartitionTransactionTransition,
};
pub use model::{
    ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES, AbortPartitionTransactionPlan,
    AbortPartitionTransactionPlanError,
};
pub use outcome::{
    AbortPartitionTransactionBrokerError, AbortPartitionTransactionFailure,
    AbortPartitionTransactionFailureKind, AbortPartitionTransactionTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
