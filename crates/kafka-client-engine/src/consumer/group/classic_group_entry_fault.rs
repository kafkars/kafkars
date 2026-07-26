//! One linear freeze point for a classic-group entry invariant failure.

use kafka_client_core::{ClassicGeneration, ClassicHeartbeatAttempt, MembershipCycle};

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationInstallFailure, ClassicCoordinatorInvalidationTerminalFailure,
    ClassicHeartbeatAdmissionFailure, ClassicHeartbeatRestoreFailure, ClassicHeartbeatTerminal,
    JoinGroupRestoreFailure, JoinGroupTerminal, SyncGroupAdmissionFailure, SyncGroupRestoreFailure,
    SyncGroupTerminal,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailure,
    classic_group_heartbeat::ClassicHeartbeatAcceptanceFailure,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_join_call::ClassicGroupJoinAcceptanceFailure,
    classic_group_partition_count_failure::ClassicGroupPartitionCountFault,
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
    SyncConfirmationTerminal(SyncGroupTerminal),
    SyncPostCore(SyncGroupTerminal),
    SyncRejectionPostCore {
        rejection: ClassicRejectionPostCore,
        terminal: SyncGroupTerminal,
    },
    SyncRecoverySemantic(MembershipCycle),
    HeartbeatAdmission(ClassicHeartbeatAdmissionFailure),
    HeartbeatAcceptance(ClassicHeartbeatAcceptanceFailure),
    HeartbeatTerminal(ClassicHeartbeatRestoreFailure),
    HeartbeatPostCore(ClassicHeartbeatTerminal),
    HeartbeatRejectionPostCore {
        rejection: ClassicRejectionPostCore,
        terminal: ClassicHeartbeatTerminal,
    },
    HeartbeatLocalRevoke {
        failure: ClassicGroupAssignmentPreparationFailure,
        generation: ClassicGeneration,
    },
    HeartbeatAdmissionRevoke {
        failure: ClassicGroupAssignmentPreparationFailure,
        generation: ClassicGeneration,
        admission: ClassicHeartbeatAdmissionFailure,
    },
    HeartbeatTerminalRevoke {
        failure: ClassicGroupAssignmentPreparationFailure,
        generation: ClassicGeneration,
        terminal: ClassicHeartbeatTerminal,
    },
    HeartbeatRecoverySemantic(ClassicHeartbeatAttempt),
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
            Self::SyncRecoverySemantic(owner) => retained_one(owner),
            Self::SyncInstall {
                failure,
                generation,
                terminal,
            } => retained_guarded_pair(failure, generation, terminal),
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
            Self::HeartbeatLocalRevoke {
                failure,
                generation,
            } => retained_one_with_guard(failure, generation),
            Self::HeartbeatAdmissionRevoke {
                failure,
                generation,
                admission,
            } => retained_guarded_pair(failure, generation, admission),
            Self::HeartbeatTerminalRevoke {
                failure,
                generation,
                terminal,
            } => retained_guarded_pair(failure, generation, terminal),
            Self::HeartbeatRecoverySemantic(attempt) => retained_one(attempt),
            Self::CoordinatorInvalidationInstall(owner) => retained_one(owner),
            Self::CoordinatorInvalidationTerminal(failure) => retained_one(failure),
            Self::CoordinatorInvalidationGate => 1,
        }
    }
}

const fn retained_one<T>(_owner: &T) -> usize {
    1
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
