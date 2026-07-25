//! One linear freeze point for a classic-group entry invariant failure.

use kafka_client_core::{ClassicGeneration, ClassicHeartbeatAttempt, MembershipCycle};

use crate::driver::classic_group::{
    ClassicHeartbeatAdmissionFailure, ClassicHeartbeatRestoreFailure, ClassicHeartbeatTerminal,
    JoinGroupRestoreFailure, JoinGroupTerminal, SyncGroupAdmissionFailure, SyncGroupRestoreFailure,
    SyncGroupTerminal,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailure,
    classic_group_heartbeat::ClassicHeartbeatAcceptanceFailure,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_join_call::ClassicGroupJoinAcceptanceFailure,
    classic_group_sync::ClassicGroupSyncAcceptanceFailure,
};

/// One first-fault owner; a faulted entry cannot attempt another membership action.
#[must_use = "a classic-group entry fault retains linear ownership until shutdown"]
#[expect(
    clippy::large_enum_variant,
    reason = "fault variants retain exact linear generated owners without another allocation"
)]
pub(super) enum ClassicGroupEntryFault {
    JoinAcceptance(ClassicGroupJoinAcceptanceFailure),
    JoinTerminal(JoinGroupRestoreFailure),
    JoinSuccessor(ClassicGroupJoinSuccessor),
    JoinSuccessorRestore {
        successor: ClassicGroupJoinSuccessor,
        failure: JoinGroupRestoreFailure,
    },
    JoinPostCore(JoinGroupTerminal),
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
    SyncRecoverySemantic(MembershipCycle),
    HeartbeatAdmission(ClassicHeartbeatAdmissionFailure),
    HeartbeatAcceptance(ClassicHeartbeatAcceptanceFailure),
    HeartbeatTerminal(ClassicHeartbeatRestoreFailure),
    HeartbeatPostCore(ClassicHeartbeatTerminal),
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
}

impl ClassicGroupEntryFault {
    pub(super) const fn retained_owner_count(&self) -> usize {
        match self {
            Self::JoinAcceptance(owner) => {
                let _ = owner;
                1
            }
            Self::JoinTerminal(owner) => {
                let _ = owner;
                1
            }
            Self::JoinSuccessor(owner) => {
                let _ = owner;
                1
            }
            Self::JoinSuccessorRestore { successor, failure } => {
                let _ = (successor, failure);
                2
            }
            Self::JoinPostCore(owner) => {
                let _ = owner;
                1
            }
            Self::SyncAcceptance(owner) => {
                let _ = owner;
                1
            }
            Self::SyncSubmission(owner) => {
                let _ = owner;
                1
            }
            Self::SyncTerminal(owner) => {
                let _ = owner;
                1
            }
            Self::SyncConfirmationTerminal(owner) | Self::SyncPostCore(owner) => {
                let _ = owner;
                1
            }
            Self::SyncRecoverySemantic(owner) => {
                let _ = owner;
                1
            }
            Self::SyncInstall {
                failure,
                generation,
                terminal,
            } => {
                let _ = (failure, generation, terminal);
                2
            }
            Self::HeartbeatAdmission(owner) => {
                let _ = owner;
                1
            }
            Self::HeartbeatAcceptance(owner) => owner.retained_owner_count(),
            Self::HeartbeatTerminal(owner) => {
                let _ = owner;
                1
            }
            Self::HeartbeatPostCore(owner) => {
                let _ = owner;
                1
            }
            Self::HeartbeatLocalRevoke {
                failure,
                generation,
            } => {
                let _ = (failure, generation);
                1
            }
            Self::HeartbeatAdmissionRevoke {
                failure,
                generation,
                admission,
            } => {
                let _ = (failure, generation, admission);
                2
            }
            Self::HeartbeatTerminalRevoke {
                failure,
                generation,
                terminal,
            } => {
                let _ = (failure, generation, terminal);
                2
            }
            Self::HeartbeatRecoverySemantic(attempt) => {
                let _ = attempt;
                1
            }
        }
    }
}
