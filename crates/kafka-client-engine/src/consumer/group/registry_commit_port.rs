//! Capture-first private port admission for classic-group offset commits.

use std::time::Duration;

use kafka_client_core::{GroupCheckpoint, GroupId};

use crate::clock::{ClockError, DeadlineCapture};

use super::{
    offset_commit::AcceptedGroupOffsetCommit,
    registry_commit::{GroupConsumerCommitFailure, GroupConsumerCommitFailureKind},
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    registry_wake::GroupConsumerShardWakeError,
};

impl GroupConsumerPort {
    pub(in crate::consumer::group) fn try_commit(
        &self,
        group_id: GroupId,
        timeout: Duration,
        checkpoint: GroupCheckpoint,
    ) -> Result<GroupConsumerCommitAdmission, GroupConsumerCommitPortFailure> {
        let capture = match self.clock.capture_deadline_after(timeout) {
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

    fn admit_captured_commit(
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
pub(super) struct GroupConsumerCommitAdmission {
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
}

/// Port-local rejection retaining the exact caller checkpoint.
#[must_use = "commit rejection retains the exact caller checkpoint"]
pub(super) struct GroupConsumerCommitPortFailure {
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

    pub(super) fn into_checkpoint(self) -> GroupCheckpoint {
        self.checkpoint
    }
}

/// Clock, admission-fence, shard, or concrete registry rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCommitPortFailureKind {
    Clock(ClockError),
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerCommitFailureKind),
}
