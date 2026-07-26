//! Declarative boundary for assignment-fenced group `OffsetFetch` RPC ownership.

mod admission;
#[cfg(test)]
mod admission_test;
mod calls;
#[cfg(test)]
mod calls_test;
mod key;
#[cfg(test)]
mod key_test;
mod recovery;
#[cfg(test)]
mod recovery_test;
mod settlement;
mod settlement_owner;
#[cfg(test)]
mod settlement_owner_test;
#[cfg(test)]
mod settlement_test;
mod submission;
#[cfg(test)]
mod submission_test;
mod terminal;
#[cfg(test)]
mod terminal_test;

pub(crate) use admission::{
    GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchAdmission,
    GroupPositionOffsetFetchAdmissionFailure, GroupPositionOffsetFetchReturn,
    GroupPositionOffsetFetchReturnReason,
};
pub(crate) use calls::TrackedGroupPositionOffsetFetchCalls;
pub(crate) use key::GroupPositionOffsetFetchKey;
pub(crate) use recovery::{
    GroupPositionOffsetFetchCompletionFailureKind, GroupPositionOffsetFetchCompletionObservation,
    GroupPositionOffsetFetchShutdownRecovery,
};
pub(crate) use settlement::{
    GroupPositionOffsetFetchBeginError, GroupPositionOffsetFetchConfirmationFailure,
    GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchRestoreFailure,
};
pub(crate) use submission::GroupPositionOffsetFetchSubmitError;
pub(crate) use terminal::GroupPositionOffsetFetchTerminal;
