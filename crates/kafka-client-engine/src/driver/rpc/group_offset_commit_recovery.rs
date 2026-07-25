//! Lossless admission and completion-fault recovery for group commit calls.

use kafka_client_core::{GroupOffsetCommitInput, OperationId};
use kafka_driver::CompletionError;

use crate::protocol::consumer::PreparedGroupOffsetCommit;

use super::group_offset_commit_submission::GroupOffsetCommitSubmitError;

/// Definitely-unsent rejection retaining the exact prepared owner.
#[must_use = "driver admission rejection still owns the prepared group commit"]
#[derive(Debug)]
pub(crate) struct GroupOffsetCommitAdmissionFailure {
    prepared: PreparedGroupOffsetCommit,
    source: GroupOffsetCommitSubmitError,
}

impl GroupOffsetCommitAdmissionFailure {
    pub(super) const fn new(
        prepared: PreparedGroupOffsetCommit,
        source: GroupOffsetCommitSubmitError,
    ) -> Self {
        Self { prepared, source }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        PreparedGroupOffsetCommit,
        GroupOffsetCommitInput,
        GroupOffsetCommitSubmitError,
    ) {
        (
            self.prepared,
            GroupOffsetCommitInput::DriverRejected,
            self.source,
        )
    }
}

/// Copyable observation of completion-channel corruption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupOffsetCommitCompletionObservation {
    operation_id: OperationId,
    source: CompletionError,
}

impl GroupOffsetCommitCompletionObservation {
    pub(crate) const fn operation_id(self) -> OperationId {
        self.operation_id
    }

    pub(crate) const fn source(self) -> CompletionError {
        self.source
    }
}

#[must_use = "completion failure retains an accepted group commit"]
pub(super) struct GroupOffsetCommitCompletionFailure {
    prepared: PreparedGroupOffsetCommit,
    observation: GroupOffsetCommitCompletionObservation,
}

impl GroupOffsetCommitCompletionFailure {
    pub(super) const fn new(prepared: PreparedGroupOffsetCommit, source: CompletionError) -> Self {
        let observation = GroupOffsetCommitCompletionObservation {
            operation_id: prepared.operation_id(),
            source,
        };
        Self {
            prepared,
            observation,
        }
    }

    pub(super) const fn observation(&self) -> GroupOffsetCommitCompletionObservation {
        self.observation
    }

    pub(super) fn into_recovery(self) -> GroupOffsetCommitCompletionRecovery {
        GroupOffsetCommitCompletionRecovery {
            prepared: self.prepared,
            observation: self.observation,
        }
    }
}

/// Post-driver-shutdown ownership of one corrupted completion.
#[must_use = "completion recovery retains an accepted group commit"]
pub(crate) struct GroupOffsetCommitCompletionRecovery {
    prepared: PreparedGroupOffsetCommit,
    observation: GroupOffsetCommitCompletionObservation,
}

/// Settled normalized input recovered only after driver ownership ended.
#[must_use = "settled recovery retains an unconsumed group commit input"]
pub(crate) struct RecoveredGroupOffsetCommitSettlement {
    operation_id: OperationId,
    input: GroupOffsetCommitInput,
}

impl RecoveredGroupOffsetCommitSettlement {
    pub(super) const fn new(operation_id: OperationId, input: GroupOffsetCommitInput) -> Self {
        Self {
            operation_id,
            input,
        }
    }

    pub(crate) fn into_parts(self) -> (OperationId, GroupOffsetCommitInput) {
        (self.operation_id, self.input)
    }
}

/// Complete post-driver recovery of every retained group commit state.
#[must_use = "shutdown recovery retains accepted group commit ownership"]
pub(crate) struct GroupOffsetCommitShutdownRecovery {
    active: Vec<PreparedGroupOffsetCommit>,
    settled: Option<RecoveredGroupOffsetCommitSettlement>,
    pending_operation_id: Option<OperationId>,
    completion: Option<GroupOffsetCommitCompletionRecovery>,
}

impl GroupOffsetCommitShutdownRecovery {
    pub(super) const fn new(
        active: Vec<PreparedGroupOffsetCommit>,
        settled: Option<RecoveredGroupOffsetCommitSettlement>,
        pending_operation_id: Option<OperationId>,
        completion: Option<GroupOffsetCommitCompletionRecovery>,
    ) -> Self {
        Self {
            active,
            settled,
            pending_operation_id,
            completion,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<PreparedGroupOffsetCommit>,
        Option<RecoveredGroupOffsetCommitSettlement>,
        Option<OperationId>,
        Option<GroupOffsetCommitCompletionRecovery>,
    ) {
        (
            self.active,
            self.settled,
            self.pending_operation_id,
            self.completion,
        )
    }
}

impl GroupOffsetCommitCompletionRecovery {
    pub(crate) fn into_parts(
        self,
    ) -> (
        PreparedGroupOffsetCommit,
        GroupOffsetCommitCompletionObservation,
    ) {
        (self.prepared, self.observation)
    }
}
