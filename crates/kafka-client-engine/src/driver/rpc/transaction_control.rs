//! Declarative boundary for transaction-coordinator control calls.

mod end;
#[cfg(test)]
mod end_test;
mod failure;
#[cfg(test)]
mod failure_test;
mod submission;
#[cfg(test)]
mod submission_test;

pub(crate) use end::{TransactionEndCall, TransactionEndTerminalFact};
pub(crate) use failure::TransactionControlDriverFailureKind;
