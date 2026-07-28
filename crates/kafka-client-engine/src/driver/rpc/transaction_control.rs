//! Declarative boundary for transaction-coordinator control calls.

mod add_partitions;
mod add_partitions_refresh;
#[cfg(test)]
mod add_partitions_refresh_test;
#[cfg(test)]
mod add_partitions_test;
mod end;
#[cfg(test)]
mod end_test;
mod failure;
#[cfg(test)]
mod failure_test;
mod submission;
#[cfg(test)]
mod submission_test;

pub(crate) use add_partitions::{
    TransactionAddPartitionsCall, TransactionAddPartitionsTerminal,
    TransactionAddPartitionsTerminalFact, TransactionPartitionTarget,
};
pub(crate) use add_partitions_refresh::TransactionAddPartitionsPoll;
pub(crate) use end::{TransactionEndCall, TransactionEndTerminalFact};
pub(crate) use failure::TransactionControlDriverFailureKind;
pub(in crate::driver::rpc) use submission::transaction_control_route;
