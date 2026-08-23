//! Declarative facade for one exact hosted `ShareAcknowledge` execution.

mod recovery;
mod settlement;
mod submission;
mod types;

#[cfg(test)]
mod execution_test;

pub(in crate::consumer::share) use types::{
    ActiveShareAcknowledgementCall, ShareAcknowledgementExecutionPoll,
    ShareAcknowledgementOwnershipFault, ShareAcknowledgementSubmissionTurn,
};
pub(in crate::consumer) use types::{
    ShareAcknowledgementExecutionFailureKind, ShareAcknowledgementExecutionOutcome,
};
