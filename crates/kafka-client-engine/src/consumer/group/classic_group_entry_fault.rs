//! One linear freeze point for a classic-group entry invariant failure.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicHeartbeatAttempt, ClassicProcessingLeaseError,
    ClassicProcessingLeaseExpiration, LiveGroupAssignment, MembershipCycle,
};

use crate::driver::{
    GroupPositionOffsetFetchRestoreFailure, GroupPositionOffsetFetchTerminal,
    classic_group::{
        ClassicCoordinatorInvalidationInstallFailure,
        ClassicCoordinatorInvalidationTerminalFailure, ClassicHeartbeatAdmissionFailure,
        ClassicHeartbeatRestoreFailure, ClassicHeartbeatTerminal, JoinGroupRestoreFailure,
        JoinGroupTerminal, SyncGroupAdmissionFailure, SyncGroupRestoreFailure, SyncGroupTerminal,
    },
};

use super::{
    classic_group_assignment::{
        ClassicGroupAssignmentPreparationFailure, ClassicGroupRevocationFailure,
    },
    classic_group_fetch::{
        ClassicGroupFetchOwnerFaultKind, ClassicGroupFetchRetirementError,
        ClassicGroupFetchTransferError,
    },
    classic_group_heartbeat::ClassicHeartbeatAcceptanceFailure,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_join_call::ClassicGroupJoinAcceptanceFailure,
    classic_group_partition_count_failure::ClassicGroupPartitionCountFault,
    classic_group_position::{
        ClassicGroupPositionAcceptanceFailure, ClassicGroupPositionExecutionError,
        ClassicGroupPositionFailure, ClassicGroupPositionPreparationError,
        ClassicGroupPositionRejectionFailure, ClassicGroupPositionTerminalApplicationFailure,
    },
    classic_group_rejection_fault::ClassicRejectionPostCore,
    classic_group_rejoin_fault::ClassicRejoinPostCore,
    classic_group_sync::ClassicGroupSyncAcceptanceFailure,
};

/// One first-fault owner; a faulted entry cannot attempt another membership action.
#[must_use = "a classic-group entry fault retains linear ownership until shutdown"]
pub(super) enum ClassicGroupEntryFault {
    JoinAcceptance(ClassicGroupJoinAcceptanceFailure),
    JoinTerminal(JoinGroupRestoreFailure),
    JoinSuccessor(ClassicGroupJoinSuccessor),
    JoinSuccessorRestore {
        successor: ClassicGroupJoinSuccessor,
        failure: JoinGroupRestoreFailure,
    },
    JoinPostCore(JoinGroupTerminal),
    JoinRejectionPostCore {
        rejection: ClassicRejectionPostCore,
        terminal: JoinGroupTerminal,
    },
    RejoinPostCore(ClassicRejoinPostCore),
    PartitionCount(ClassicGroupPartitionCountFault),
    SyncAcceptance(ClassicGroupSyncAcceptanceFailure),
    SyncSubmission(SyncGroupAdmissionFailure),
    SyncTerminal(SyncGroupRestoreFailure),
    SyncInstall {
        failure: ClassicGroupAssignmentPreparationFailure,
        generation: ClassicGeneration,
        terminal: SyncGroupTerminal,
    },
    SyncPositionPreparation {
        terminal: SyncGroupTerminal,
        error: ClassicGroupPositionPreparationError,
    },
    SyncProcessingLeaseActivation {
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
        terminal: SyncGroupTerminal,
        error: ClassicProcessingLeaseError,
    },
    SyncConfirmationTerminal(SyncGroupTerminal),
    SyncPostCore(SyncGroupTerminal),
    SyncRejectionPostCore {
        rejection: ClassicRejectionPostCore,
        terminal: SyncGroupTerminal,
    },
    ClassicReconciliationPostCore {
        requires_followup: bool,
        first: Option<ClassicGroupEffect>,
        second: Option<ClassicGroupEffect>,
    },
    ConsumerGroupPositionPreparation {
        assignment: LiveGroupAssignment,
        error: ClassicGroupPositionPreparationError,
    },
    ConsumerGroupProcessingLeaseActivation {
        assignment: LiveGroupAssignment,
        error: ClassicProcessingLeaseError,
    },
    ConsumerGroupProcessingLeaseRevocation(ClassicProcessingLeaseError),
    ConsumerGroupFetchRetirement(ClassicGroupFetchRetirementError),
    SyncRecoverySemantic(MembershipCycle),
    PositionAcceptance(ClassicGroupPositionAcceptanceFailure),
    PositionRejection(ClassicGroupPositionRejectionFailure),
    PositionSubmission {
        fence: kafka_client_core::GroupPositionFence,
        error: ClassicGroupPositionExecutionError,
    },
    PositionDuplicateFence(kafka_client_core::GroupPositionFence),
    PositionTerminalRestore(GroupPositionOffsetFetchRestoreFailure),
    PositionTerminalPostCore {
        failure: ClassicGroupPositionTerminalApplicationFailure,
        terminal: GroupPositionOffsetFetchTerminal,
    },
    PositionFailure(ClassicGroupPositionFailure),
    FetchTransfer(ClassicGroupFetchTransferError),
    FetchOwner(ClassicGroupFetchOwnerFaultKind),
    HeartbeatAdmission(ClassicHeartbeatAdmissionFailure),
    HeartbeatAcceptance(ClassicHeartbeatAcceptanceFailure),
    HeartbeatTerminal(ClassicHeartbeatRestoreFailure),
    HeartbeatPostCore(ClassicHeartbeatTerminal),
    HeartbeatRejectionPostCore {
        rejection: ClassicRejectionPostCore,
        terminal: ClassicHeartbeatTerminal,
    },
    HeartbeatLocalRevoke {
        failure: ClassicGroupRevocationFailure,
    },
    HeartbeatAdmissionRevoke {
        failure: ClassicGroupRevocationFailure,
        admission: ClassicHeartbeatAdmissionFailure,
    },
    HeartbeatTerminalRevoke {
        failure: ClassicGroupRevocationFailure,
        terminal: ClassicHeartbeatTerminal,
    },
    HeartbeatRecoverySemantic(ClassicHeartbeatAttempt),
    ProcessingSemantic(ClassicProcessingLeaseExpiration),
    ProcessingPostCore {
        expiration: ClassicProcessingLeaseExpiration,
        first: Option<ClassicGroupEffect>,
        second: Option<ClassicGroupEffect>,
    },
    ProcessingRevoke {
        expiration: ClassicProcessingLeaseExpiration,
        failure: ClassicGroupRevocationFailure,
    },
    CoordinatorInvalidationInstall(ClassicCoordinatorInvalidationInstallFailure),
    CoordinatorInvalidationTerminal(ClassicCoordinatorInvalidationTerminalFailure),
    CoordinatorInvalidationGate,
}

impl ClassicGroupEntryFault {
    pub(super) fn retained_owner_count(&self) -> usize {
        match self {
            Self::JoinAcceptance(owner) => retained_one(owner),
            Self::JoinTerminal(owner) => retained_one(owner),
            Self::JoinSuccessor(owner) => retained_one(owner),
            Self::JoinSuccessorRestore { successor, failure } => retained_pair(successor, failure),
            Self::JoinPostCore(owner) => retained_one(owner),
            Self::JoinRejectionPostCore {
                rejection,
                terminal,
            } => rejection
                .retained_owner_count()
                .saturating_add(retained_one(terminal)),
            Self::RejoinPostCore(owner) => owner.retained_owner_count(),
            Self::PartitionCount(fault) => retained_one(fault),
            Self::SyncAcceptance(owner) => retained_one(owner),
            Self::SyncSubmission(owner) => retained_one(owner),
            Self::SyncTerminal(owner) => retained_one(owner),
            Self::SyncConfirmationTerminal(owner) | Self::SyncPostCore(owner) => {
                retained_one(owner)
            }
            Self::SyncRejectionPostCore {
                rejection,
                terminal,
            } => rejection
                .retained_owner_count()
                .saturating_add(retained_one(terminal)),
            Self::ClassicReconciliationPostCore {
                requires_followup,
                first,
                second,
            } => {
                retained_reconciliation_effects(*requires_followup, first.as_ref(), second.as_ref())
            }
            Self::ConsumerGroupPositionPreparation { assignment, error } => {
                retained_one_with_guard(assignment, error)
            }
            Self::ConsumerGroupProcessingLeaseActivation { assignment, error } => {
                retained_one_with_guard(assignment, error)
            }
            Self::ConsumerGroupProcessingLeaseRevocation(error) => retained_marker(error),
            Self::ConsumerGroupFetchRetirement(error) => retained_marker(error),
            Self::SyncRecoverySemantic(owner) => retained_one(owner),
            Self::PositionAcceptance(owner) => owner.retained_owner_count(),
            Self::PositionRejection(owner) => owner.retained_owner_count(),
            Self::PositionSubmission { fence, error } => retained_one_with_guard(fence, error),
            Self::PositionDuplicateFence(fence) => retained_one(fence),
            Self::PositionTerminalRestore(owner) => retained_one(owner),
            Self::PositionTerminalPostCore { failure, terminal } => {
                retained_pair(failure, terminal)
            }
            Self::PositionFailure(failure) => failure.retained_owner_count(),
            Self::FetchTransfer(owner) => retained_one(owner),
            Self::FetchOwner(owner) => retained_one(owner),
            Self::SyncInstall {
                failure,
                generation,
                terminal,
            } => retained_guarded_pair(failure, generation, terminal),
            Self::SyncPositionPreparation { terminal, error } => {
                retained_one_with_guard(terminal, error)
            }
            Self::SyncProcessingLeaseActivation {
                assignment,
                generation,
                terminal,
                error,
            } => {
                let _ = error;
                retained_guarded_pair(assignment, generation, terminal)
            }
            Self::HeartbeatAdmission(owner) => retained_one(owner),
            Self::HeartbeatAcceptance(owner) => owner.retained_owner_count(),
            Self::HeartbeatTerminal(owner) => retained_one(owner),
            Self::HeartbeatPostCore(owner) => retained_one(owner),
            Self::HeartbeatRejectionPostCore {
                rejection,
                terminal,
            } => rejection
                .retained_owner_count()
                .saturating_add(retained_one(terminal)),
            Self::HeartbeatLocalRevoke { failure } => retained_one(failure),
            Self::HeartbeatAdmissionRevoke { failure, admission } => {
                retained_pair(failure, admission)
            }
            Self::HeartbeatTerminalRevoke { failure, terminal } => retained_pair(failure, terminal),
            Self::HeartbeatRecoverySemantic(attempt) => retained_one(attempt),
            Self::ProcessingSemantic(owner) => retained_one(owner),
            Self::ProcessingPostCore {
                expiration,
                first,
                second,
            } => retained_guarded_pair(expiration, first, second),
            Self::ProcessingRevoke {
                expiration,
                failure,
            } => retained_pair(expiration, failure),
            Self::CoordinatorInvalidationInstall(owner) => retained_one(owner),
            Self::CoordinatorInvalidationTerminal(failure) => retained_one(failure),
            Self::CoordinatorInvalidationGate => 1,
        }
    }
}

fn retained_reconciliation_effects(
    _requires_followup: bool,
    first: Option<&ClassicGroupEffect>,
    second: Option<&ClassicGroupEffect>,
) -> usize {
    usize::from(first.is_some()).saturating_add(usize::from(second.is_some()))
}

const fn retained_one<T>(_owner: &T) -> usize {
    1
}

const fn retained_marker<T>(_marker: &T) -> usize {
    0
}

const fn retained_pair<T, U>(_first: &T, _second: &U) -> usize {
    2
}

const fn retained_one_with_guard<T, U>(_owner: &T, _guard: &U) -> usize {
    1
}

const fn retained_guarded_pair<T, U, V>(_first: &T, _guard: &U, _second: &V) -> usize {
    2
}
