//! Atomic capacity reservation and catalog-fenced core admission.

use kafka_client_core::{GroupCheckpoint, GroupOffsetCommitMachine};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionObserver, CompletionRegistryError},
    protocol::consumer::{GroupOffsetCommitEntryReservation, GroupOffsetCommitResultReservation},
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    host::{
        AcceptedGroupOffsetCommit, GROUP_OFFSET_COMMIT_OPERATION_BYTES,
        GROUP_OFFSET_COMMIT_RETAINED_BYTES, GroupOffsetCommitHost,
    },
    snapshot::PreparedSnapshot,
};

/// Local rejection retaining the exact linear checkpoint.
#[must_use = "group offset commit rejection retains the caller checkpoint"]
pub(super) struct GroupOffsetCommitAdmissionFailure {
    pub(super) kind: GroupOffsetCommitAdmissionFailureKind,
    pub(super) checkpoint: GroupCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetCommitAdmissionFailureKind {
    Closed,
    Capacity,
    RetainedBytes,
    ResultCapacity,
    SnapshotCapacity,
    Core(kafka_client_core::GroupOffsetCommitAdmissionErrorKind),
    HostUnavailable,
}

struct ReservedAdmission {
    checkpoint: GroupCheckpoint,
    completion_id: CompletionId,
    observer: CompletionObserver<kafka_client_core::GroupOffsetCommitTerminal>,
    entry_capacity: GroupOffsetCommitEntryReservation,
    result_capacity: GroupOffsetCommitResultReservation,
    snapshot: PreparedSnapshot,
}

impl GroupOffsetCommitHost {
    pub(super) fn try_admit(
        &mut self,
        catalog: &GroupSessionCatalog,
        deadline: OperationDeadline,
        checkpoint: GroupCheckpoint,
    ) -> Result<AcceptedGroupOffsetCommit, GroupOffsetCommitAdmissionFailure> {
        if !self.accepting {
            return Err(failure(
                GroupOffsetCommitAdmissionFailureKind::Closed,
                checkpoint,
            ));
        }
        if !self.is_available_for_admission() {
            return Err(failure(
                GroupOffsetCommitAdmissionFailureKind::HostUnavailable,
                checkpoint,
            ));
        }
        let Some(operation_id) = self.next_operation_id else {
            return Err(failure(
                GroupOffsetCommitAdmissionFailureKind::Capacity,
                checkpoint,
            ));
        };
        let reserved = self.reserve_before_core(catalog, checkpoint)?;
        let admission = match GroupOffsetCommitMachine::try_admit(
            operation_id,
            deadline.core(),
            catalog.live_assignment(),
            reserved.checkpoint,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                self.retained_bytes -= GROUP_OFFSET_COMMIT_OPERATION_BYTES;
                return Err(self.rollback_core_admission(
                    reserved.completion_id,
                    reserved.observer,
                    error,
                ));
            }
        };
        self.next_operation_id = operation_id
            .get()
            .checked_add(1)
            .map(kafka_client_core::OperationId::from_raw);
        let (machine, effect) = admission.into_parts();
        self.install_admitted(
            operation_id,
            deadline,
            machine,
            effect,
            reserved.snapshot,
            reserved.entry_capacity,
            reserved.result_capacity,
            reserved.completion_id,
            reserved.observer,
        )
    }

    fn reserve_before_core(
        &mut self,
        catalog: &GroupSessionCatalog,
        checkpoint: GroupCheckpoint,
    ) -> Result<ReservedAdmission, GroupOffsetCommitAdmissionFailure> {
        let Some(total_bytes) = self
            .retained_bytes
            .checked_add(GROUP_OFFSET_COMMIT_OPERATION_BYTES)
        else {
            return Err(failure(
                GroupOffsetCommitAdmissionFailureKind::RetainedBytes,
                checkpoint,
            ));
        };
        if total_bytes > GROUP_OFFSET_COMMIT_RETAINED_BYTES {
            return Err(failure(
                GroupOffsetCommitAdmissionFailureKind::RetainedBytes,
                checkpoint,
            ));
        }
        let (completion_id, observer) = match self.completions.reserve() {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(failure(completion_failure(error), checkpoint));
            }
        };
        let entry_count = checkpoint.entries().len();
        let entry_capacity = match GroupOffsetCommitEntryReservation::try_new(entry_count) {
            Ok(capacity) => capacity,
            Err(_error) => {
                return Err(self.rollback_admission(
                    completion_id,
                    observer,
                    checkpoint,
                    GroupOffsetCommitAdmissionFailureKind::ResultCapacity,
                ));
            }
        };
        let result_capacity = match GroupOffsetCommitResultReservation::try_new(entry_count) {
            Ok(capacity) => capacity,
            Err(_error) => {
                return Err(self.rollback_admission(
                    completion_id,
                    observer,
                    checkpoint,
                    GroupOffsetCommitAdmissionFailureKind::ResultCapacity,
                ));
            }
        };
        let mut topic_names = Vec::new();
        if topic_names.try_reserve_exact(entry_count).is_err() {
            return Err(self.rollback_admission(
                completion_id,
                observer,
                checkpoint,
                GroupOffsetCommitAdmissionFailureKind::SnapshotCapacity,
            ));
        }
        let snapshot = match Self::snapshot(catalog, &checkpoint, topic_names) {
            Ok(snapshot) => snapshot,
            Err(_error) => {
                return Err(self.rollback_admission(
                    completion_id,
                    observer,
                    checkpoint,
                    GroupOffsetCommitAdmissionFailureKind::SnapshotCapacity,
                ));
            }
        };
        if !pre_core_charge_fits(&entry_capacity, &result_capacity, &snapshot) {
            return Err(self.rollback_admission(
                completion_id,
                observer,
                checkpoint,
                GroupOffsetCommitAdmissionFailureKind::RetainedBytes,
            ));
        }
        self.retained_bytes = total_bytes;
        Ok(ReservedAdmission {
            checkpoint,
            completion_id,
            observer,
            entry_capacity,
            result_capacity,
            snapshot,
        })
    }
}

fn pre_core_charge_fits(
    entry_capacity: &GroupOffsetCommitEntryReservation,
    result_capacity: &GroupOffsetCommitResultReservation,
    snapshot: &PreparedSnapshot,
) -> bool {
    entry_capacity
        .reserved_bytes()
        .and_then(|bytes| {
            result_capacity
                .outcomes_capacity()
                .checked_mul(core::mem::size_of::<
                    kafka_client_core::GroupOffsetCommitPartitionOutcome,
                >())
                .and_then(|result_bytes| bytes.checked_add(result_bytes))
        })
        .and_then(|bytes| bytes.checked_add(snapshot.request.retained_bytes()))
        .is_some_and(|bytes| bytes <= GROUP_OFFSET_COMMIT_OPERATION_BYTES)
}

fn completion_failure(error: CompletionRegistryError) -> GroupOffsetCommitAdmissionFailureKind {
    match error {
        CompletionRegistryError::Full => GroupOffsetCommitAdmissionFailureKind::Capacity,
        _ => GroupOffsetCommitAdmissionFailureKind::HostUnavailable,
    }
}

pub(super) fn failure(
    kind: GroupOffsetCommitAdmissionFailureKind,
    checkpoint: GroupCheckpoint,
) -> GroupOffsetCommitAdmissionFailure {
    GroupOffsetCommitAdmissionFailure { kind, checkpoint }
}
