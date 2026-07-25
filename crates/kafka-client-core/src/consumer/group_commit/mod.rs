//! Assignment-fenced group checkpoints and per-operation commit decisions.

mod assignment;
mod checkpoint;
mod effect;
mod identity;
mod input;
mod machine;
mod outcome;
mod validation;

pub use assignment::{
    GroupAssignmentPartition, GroupOffsetCommitAdmissionError, GroupOffsetCommitAdmissionErrorKind,
    LiveGroupAssignment, LiveGroupAssignmentError,
};
pub use checkpoint::{
    GroupCheckpoint, GroupCheckpointEntry, GroupCheckpointEntryError, GroupCheckpointError,
};
pub use effect::{
    GroupOffsetCommitAdmission, GroupOffsetCommitEffect, GroupOffsetCommitTransition,
};
pub use identity::{AssignmentGeneration, GroupId, MemberId};
pub use input::{
    GroupOffsetCommitApplyError, GroupOffsetCommitInput, GroupOffsetCommitMachineError,
    GroupOffsetCommitState,
};
pub use machine::GroupOffsetCommitMachine;
pub use outcome::{
    GroupOffsetCommitBatch, GroupOffsetCommitBrokerError, GroupOffsetCommitBrokerRejection,
    GroupOffsetCommitFailure, GroupOffsetCommitFailureKind, GroupOffsetCommitPartitionOutcome,
    GroupOffsetCommitPartitionResult, GroupOffsetCommitTerminal,
};
pub use validation::validate_group_offset_commit_checkpoint;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod apply_error_test;
#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod checkpoint_test;
#[cfg(test)]
mod effect_test;
#[cfg(test)]
mod input_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod validation_test;
