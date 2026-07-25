//! One linear freeze point for a classic-group entry invariant failure.

use kafka_client_core::{ClassicGeneration, MembershipCycle};

use crate::driver::classic_group::{
    JoinGroupRestoreFailure, JoinGroupTerminal, SyncGroupAdmissionFailure, SyncGroupRestoreFailure,
    SyncGroupTerminal,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailure,
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
        }
    }
}
