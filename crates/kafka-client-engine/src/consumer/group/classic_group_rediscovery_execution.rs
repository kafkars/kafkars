//! Bounded submission and terminal installation for core-authorized rediscovery.

use kafka_client_core::GroupId;

use crate::driver::{
    DriverOwner,
    classic_group::{
        ClassicCoordinatorInvalidationAdmissionFailureKind,
        ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationPoll,
        ClassicCoordinatorInvalidationTerminalFailure,
    },
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicCoordinatorInvalidationTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn drive_one_classic_coordinator_invalidation(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicCoordinatorInvalidationTurn, ClassicGroupExecutionError> {
        let poll = match self
            .coordinator_invalidations
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
            .drive_one(driver)
        {
            Ok(poll) => poll,
            Err(failure) => {
                return match failure.kind() {
                    ClassicCoordinatorInvalidationAdmissionFailureKind::Full => {
                        Ok(ClassicCoordinatorInvalidationTurn::Blocked)
                    }
                    ClassicCoordinatorInvalidationAdmissionFailureKind::Closed
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::Wake
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::IdentityExhausted
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::ForeignDriver
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::VersionBoundsInvalid => {
                        Err(ClassicGroupExecutionError::CoordinatorInvalidationAdmission)
                    }
                };
            }
        };
        match poll {
            ClassicCoordinatorInvalidationPoll::Idle => {
                Ok(ClassicCoordinatorInvalidationTurn::Idle)
            }
            ClassicCoordinatorInvalidationPoll::Submitted { .. } => {
                Ok(ClassicCoordinatorInvalidationTurn::Progress)
            }
            ClassicCoordinatorInvalidationPoll::Pending { .. } => {
                Ok(ClassicCoordinatorInvalidationTurn::Blocked)
            }
            ClassicCoordinatorInvalidationPoll::Terminal(terminal) => {
                self.apply_classic_coordinator_invalidation_terminal(
                    terminal.group_id(),
                    terminal.result(),
                )?;
                Ok(ClassicCoordinatorInvalidationTurn::Progress)
            }
        }
    }

    pub(super) fn apply_classic_coordinator_invalidation_terminal(
        &mut self,
        group_id: GroupId,
        result: Result<
            ClassicCoordinatorInvalidationPermission,
            ClassicCoordinatorInvalidationTerminalFailure,
        >,
    ) -> Result<(), ClassicGroupExecutionError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        match result {
            Ok(
                ClassicCoordinatorInvalidationPermission::Applied
                | ClassicCoordinatorInvalidationPermission::IgnoredStale,
            ) => entry
                .rediscovery
                .permit_rejoin()
                .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationGate),
            Err(failure) => {
                entry.fault = Some(ClassicGroupEntryFault::CoordinatorInvalidationTerminal(
                    failure,
                ));
                Err(ClassicGroupExecutionError::CoordinatorInvalidationTerminal)
            }
        }
    }
}
