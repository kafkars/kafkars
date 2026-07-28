//! Declarative boundary for tracked transactional offset calls.

mod add_offsets;
mod add_offsets_refresh;
#[cfg(test)]
mod add_offsets_test;
mod failure;
#[cfg(test)]
mod failure_test;
mod offset_commit;
mod offset_commit_refresh;
#[cfg(test)]
mod offset_commit_refresh_test;
mod offset_commit_target;
#[cfg(test)]
mod offset_commit_target_test;
#[cfg(test)]
mod offset_commit_test;
mod submission;
#[cfg(test)]
mod submission_test;

#[cfg(test)]
pub(crate) use add_offsets::TransactionAddOffsetsCallAdmissionFailure;
pub(crate) use add_offsets::{
    TransactionAddOffsetsCall, TransactionAddOffsetsPoll, TransactionAddOffsetsTerminal,
    TransactionAddOffsetsTerminalFact,
};
pub(crate) use failure::TransactionOffsetDriverFailureKind;
#[cfg(test)]
pub(crate) use offset_commit::TransactionOffsetCommitCallAdmissionFailure;
pub(crate) use offset_commit::{
    TransactionOffsetCommitCall, TransactionOffsetCommitTerminal,
    TransactionOffsetCommitTerminalFact,
};
pub(crate) use offset_commit_refresh::TransactionOffsetCommitPoll;
pub(crate) use offset_commit_target::TransactionOffsetCommitTarget;
