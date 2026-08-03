//! Catalog-selected admission into the one global offset-commit host.

use kafka_client_core::{GroupCheckpoint, GroupId};

use crate::clock::OperationDeadline;

use super::{
    offset_commit::{
        AcceptedGroupOffsetCommit, GroupOffsetCommitAdmissionFailure,
        GroupOffsetCommitAdmissionFailureKind,
    },
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};
use crate::consumer::group_registration_request::GroupConsumerProtocol;

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
        if entry.fault.is_some() {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::EntryFault,
                checkpoint,
            ));
        }
        if !entry.is_active() {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::GroupClosing,
                checkpoint,
            ));
        }
        if entry.catalog.live_assignment().is_some() && !session_matches_selected_protocol(entry) {
            return Err(commit_failure(
                GroupConsumerCommitFailureKind::EntryFault,
                checkpoint,
            ));
        }
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .try_admit(entry.protocol, &entry.catalog, deadline, checkpoint)
            .map_err(delegated_failure)
    }
}

fn session_matches_selected_protocol(entry: &GroupConsumerEntry) -> bool {
    match entry.protocol {
        GroupConsumerProtocol::Classic => {
            entry.catalog.classic_generation().is_some()
                && entry.catalog.consumer_group_member_epoch().is_none()
        }
        GroupConsumerProtocol::Consumer => {
            entry.catalog.classic_generation().is_none()
                && entry.catalog.consumer_group_member_epoch().is_some()
        }
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
