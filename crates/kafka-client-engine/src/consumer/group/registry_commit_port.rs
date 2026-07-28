//! Capture-first private port admission for classic-group offset commits.

use std::time::Duration;

use kafka_client_core::{GroupCheckpoint, GroupId};

use crate::clock::{ClockError, DeadlineCapture};

use super::{
    offset_commit::{AcceptedGroupOffsetCommit, GroupOffsetCommitAdmissionFailureKind},
    registry_commit::{GroupConsumerCommitFailure, GroupConsumerCommitFailureKind},
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    registry_wake::GroupConsumerShardWakeError,
};

impl GroupConsumerPort {
    pub(in crate::consumer) fn capture_commit_deadline(
        &self,
        timeout: Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        self.clock.capture_deadline_after(timeout)
    }

    pub(in crate::consumer) fn try_commit(
        &self,
        group_id: GroupId,
        timeout: Duration,
        checkpoint: GroupCheckpoint,
    ) -> Result<GroupConsumerCommitAdmission, GroupConsumerCommitPortFailure> {
        let capture = match self.capture_commit_deadline(timeout) {
            Ok(capture) => capture,
            Err(error) => {
                return Err(GroupConsumerCommitPortFailure::new(
                    GroupConsumerCommitPortFailureKind::Clock(error),
                    checkpoint,
                ));
            }
        };
        self.admit_captured_commit(group_id, capture, checkpoint)
    }

    pub(in crate::consumer) fn admit_captured_commit(
        &self,
        group_id: GroupId,
        capture: DeadlineCapture,
        checkpoint: GroupCheckpoint,
    ) -> Result<GroupConsumerCommitAdmission, GroupConsumerCommitPortFailure> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerCommitPortFailure::new(
                GroupConsumerCommitPortFailureKind::Closed,
                checkpoint,
            ));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(GroupConsumerCommitPortFailure::new(
                    GroupConsumerCommitPortFailureKind::Lock(error),
                    checkpoint,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerCommitPortFailure::new(
                GroupConsumerCommitPortFailureKind::Closed,
                checkpoint,
            ));
        }
        let accepted = registry
            .try_commit(group_id, capture.operation_deadline(), checkpoint)
            .map_err(GroupConsumerCommitPortFailure::registry)?;
        drop(registry);
        Ok(GroupConsumerCommitAdmission {
            accepted,
            wake: self.shared.request_turn().err(),
        })
    }
}

/// Accepted terminal observer plus an advisory post-admission wake fault.
#[must_use = "accepted commit admission retains its terminal observer"]
pub(in crate::consumer) struct GroupConsumerCommitAdmission {
    accepted: AcceptedGroupOffsetCommit,
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerCommitAdmission {
    pub(super) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        AcceptedGroupOffsetCommit,
        Option<GroupConsumerShardWakeError>,
    ) {
        (self.accepted, self.wake)
    }

    pub(in crate::consumer) fn into_public_parts(self) -> (AcceptedGroupOffsetCommit, bool) {
        let (accepted, wake) = self.into_parts();
        (accepted, wake.is_some())
    }
}

/// Port-local rejection retaining the exact caller checkpoint.
#[must_use = "commit rejection retains the exact caller checkpoint"]
pub(in crate::consumer) struct GroupConsumerCommitPortFailure {
    pub(super) kind: GroupConsumerCommitPortFailureKind,
    checkpoint: GroupCheckpoint,
}

impl GroupConsumerCommitPortFailure {
    fn new(kind: GroupConsumerCommitPortFailureKind, checkpoint: GroupCheckpoint) -> Self {
        Self { kind, checkpoint }
    }

    fn registry(failure: GroupConsumerCommitFailure) -> Self {
        Self {
            kind: GroupConsumerCommitPortFailureKind::Registry(failure.kind),
            checkpoint: failure.checkpoint,
        }
    }

    pub(in crate::consumer) fn into_checkpoint(self) -> GroupCheckpoint {
        self.checkpoint
    }

    pub(in crate::consumer) const fn public_category(
        &self,
    ) -> GroupConsumerCommitPortErrorCategory {
        match self.kind {
            GroupConsumerCommitPortFailureKind::Clock(_) => {
                GroupConsumerCommitPortErrorCategory::InvalidDeadline
            }
            GroupConsumerCommitPortFailureKind::Closed
            | GroupConsumerCommitPortFailureKind::Registry(
                GroupConsumerCommitFailureKind::RegistryClosed
                | GroupConsumerCommitFailureKind::OffsetCommit(
                    GroupOffsetCommitAdmissionFailureKind::Closed,
                ),
            ) => GroupConsumerCommitPortErrorCategory::Closed,
            GroupConsumerCommitPortFailureKind::Lock(GroupConsumerShardLockError::Contended) => {
                GroupConsumerCommitPortErrorCategory::Contended
            }
            GroupConsumerCommitPortFailureKind::Registry(
                GroupConsumerCommitFailureKind::UnknownGroup
                | GroupConsumerCommitFailureKind::GroupClosing,
            ) => GroupConsumerCommitPortErrorCategory::GroupUnavailable,
            GroupConsumerCommitPortFailureKind::Registry(
                GroupConsumerCommitFailureKind::OffsetCommit(
                    GroupOffsetCommitAdmissionFailureKind::Capacity
                    | GroupOffsetCommitAdmissionFailureKind::RetainedBytes
                    | GroupOffsetCommitAdmissionFailureKind::ResultCapacity
                    | GroupOffsetCommitAdmissionFailureKind::SnapshotCapacity
                    | GroupOffsetCommitAdmissionFailureKind::Core(
                        kafka_client_core::GroupOffsetCommitAdmissionErrorKind::AllocationFailed,
                    ),
                ),
            ) => GroupConsumerCommitPortErrorCategory::Backpressure,
            GroupConsumerCommitPortFailureKind::Registry(
                GroupConsumerCommitFailureKind::OffsetCommit(
                    GroupOffsetCommitAdmissionFailureKind::Core(
                        kafka_client_core::GroupOffsetCommitAdmissionErrorKind::AssignmentLost
                        | kafka_client_core::GroupOffsetCommitAdmissionErrorKind::GroupMismatch
                        | kafka_client_core::GroupOffsetCommitAdmissionErrorKind::MemberMismatch
                        | kafka_client_core::GroupOffsetCommitAdmissionErrorKind::GenerationMismatch
                        | kafka_client_core::GroupOffsetCommitAdmissionErrorKind::UnassignedPartition {
                            ..
                        },
                    ),
                ),
            ) => GroupConsumerCommitPortErrorCategory::StaleCheckpoint,
            GroupConsumerCommitPortFailureKind::Lock(GroupConsumerShardLockError::Poisoned)
            | GroupConsumerCommitPortFailureKind::Registry(
                GroupConsumerCommitFailureKind::EntryFault
                | GroupConsumerCommitFailureKind::OffsetCommit(
                    GroupOffsetCommitAdmissionFailureKind::HostUnavailable,
                ),
            ) => GroupConsumerCommitPortErrorCategory::HostUnavailable,
        }
    }
}

/// Stable public classification of one pre-admission commit rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCommitPortErrorCategory {
    InvalidDeadline,
    Closed,
    Contended,
    GroupUnavailable,
    Backpressure,
    StaleCheckpoint,
    HostUnavailable,
}

/// Clock, admission-fence, shard, or concrete registry rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCommitPortFailureKind {
    Clock(ClockError),
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerCommitFailureKind),
}
