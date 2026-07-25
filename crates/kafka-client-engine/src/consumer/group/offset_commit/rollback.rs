//! Completion and byte rollback before deterministic admission succeeds.

use kafka_client_core::{
    GroupCheckpoint, GroupOffsetCommitAdmissionError, GroupOffsetCommitTerminal,
};

use crate::completion::{CompletionId, CompletionObserver, CompletionRegistryError};

use super::{
    admission::{
        GroupOffsetCommitAdmissionFailure, GroupOffsetCommitAdmissionFailureKind, failure,
    },
    host::{GroupOffsetCommitHost, GroupOffsetCommitHostError},
};

impl GroupOffsetCommitHost {
    pub(super) fn rollback_admission(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<GroupOffsetCommitTerminal>,
        checkpoint: GroupCheckpoint,
        kind: GroupOffsetCommitAdmissionFailureKind,
    ) -> GroupOffsetCommitAdmissionFailure {
        if self
            .completions
            .rollback_reservation(completion_id)
            .is_err()
        {
            self.fault = Some(GroupOffsetCommitHostError::Completion(
                CompletionRegistryError::UnknownCompletion,
            ));
            drop(observer);
            return failure(
                GroupOffsetCommitAdmissionFailureKind::HostUnavailable,
                checkpoint,
            );
        }
        drop(observer);
        failure(kind, checkpoint)
    }

    pub(super) fn rollback_core_admission(
        &mut self,
        completion_id: CompletionId,
        observer: CompletionObserver<GroupOffsetCommitTerminal>,
        error: GroupOffsetCommitAdmissionError,
    ) -> GroupOffsetCommitAdmissionFailure {
        let kind = GroupOffsetCommitAdmissionFailureKind::Core(error.kind());
        self.rollback_admission(completion_id, observer, error.into_checkpoint(), kind)
    }
}
