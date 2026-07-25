//! Catalog-selected admission into the one global offset-commit host.

use kafka_client_core::{GroupCheckpoint, GroupId};

use crate::clock::OperationDeadline;

use super::{
    offset_commit::{
        AcceptedGroupOffsetCommit, GroupOffsetCommitAdmissionFailure,
        GroupOffsetCommitAdmissionFailureKind,
    },
    registry::GroupConsumerRegistry,
};

/// Registry rejection retaining the exact caller checkpoint.
#[must_use = "registry commit rejection retains the caller checkpoint"]
pub(super) struct GroupConsumerCommitFailure {
    pub(super) kind: GroupConsumerCommitFailureKind,
    pub(super) checkpoint: GroupCheckpoint,
}

/// Registry selection failure or exact delegated host rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCommitFailureKind {
    RegistryClosed,
    EntryFault,
    UnknownGroup,
    GroupClosing,
    OffsetCommit(GroupOffsetCommitAdmissionFailureKind),
}

impl GroupConsumerRegistry {
    pub(super) fn try_commit(
        &mut self,
        group_id: GroupId,
        deadline: OperationDeadline,
        checkpoint: GroupCheckpoint,
    ) -> Result<AcceptedGroupOffsetCommit, GroupConsumerCommitFailure> {
        if !self.accepting {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::RegistryClosed,
                checkpoint,
            ));
        }
        if self.has_entry_fault() {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::EntryFault,
                checkpoint,
            ));
        }
        let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::UnknownGroup,
                checkpoint,
            ));
        };
        if !entry.is_active() {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::GroupClosing,
                checkpoint,
            ));
        }
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .try_admit(&entry.catalog, deadline, checkpoint)
            .map_err(delegated_failure)
    }
}

fn delegated_failure(failure: GroupOffsetCommitAdmissionFailure) -> GroupConsumerCommitFailure {
    GroupConsumerCommitFailure {
        kind: GroupConsumerCommitFailureKind::OffsetCommit(failure.kind),
        checkpoint: failure.checkpoint,
    }
}

fn commit_failure(
    kind: GroupConsumerCommitFailureKind,
    checkpoint: GroupCheckpoint,
) -> GroupConsumerCommitFailure {
    GroupConsumerCommitFailure { kind, checkpoint }
}
