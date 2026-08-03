//! Local close policy using the execution owner's guarded state operations.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, ClassicProcessingLease,
    ClassicRejoinSchedule,
};

use super::{
    classic_group_assignment::retire_and_revoke_classic_group_assignment,
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_fetch::ClassicGroupFetchOwner,
    classic_group_heartbeat_prepare::map_revocation_kind,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_owner::ClassicGroupOwner,
    classic_group_reconciliation::PreparedClassicGroupReconciliation,
    session_catalog::GroupSessionCatalog,
};

impl ClassicGroupExecution {
    /// Closes one confirmed or recovered cooperative split-authority window.
    ///
    /// Core already owns the replacement assignment while catalog, processing,
    /// and Fetch deliberately retain the previous assignment until
    /// reconciliation commits. Closing must validate and consume both exact
    /// identities instead of feeding core's replacement `Revoke` into the
    /// previous-assignment retirement seam.
    #[expect(
        clippy::too_many_lines,
        reason = "cooperative close validates and consumes one atomic split-authority transition"
    )]
    pub(super) fn close_reconciliation_if_local(
        &mut self,
        owner: &mut ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
        installed_rejoin: Option<ClassicRejoinSchedule>,
        pending: &mut Option<PreparedClassicGroupReconciliation>,
    ) -> Result<ClassicGroupCloseProgress, ClassicGroupExecutionError> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
            | ClassicGroupExecutionState::JoinDriverOwned(_)
            | ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { .. } => {
                return Ok(ClassicGroupCloseProgress::DriverOwned);
            }
            ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::PartitionCountHandoff { .. } => {
                return Err(ClassicGroupExecutionError::HandoffIncomplete);
            }
            ClassicGroupExecutionState::Idle => {}
            ClassicGroupExecutionState::CloseFault { revoke_failure } => {
                return Err(map_revocation_kind(revoke_failure.kind));
            }
            ClassicGroupExecutionState::PartitionCountsPostCore { .. } => {
                return Err(ClassicGroupExecutionError::PartitionCountsPostCore);
            }
            ClassicGroupExecutionState::PreparedJoin(_)
            | ClassicGroupExecutionState::PreparedPartitionCounts(_)
            | ClassicGroupExecutionState::PreparedSync(_) => {
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
        }

        let supplemental_matches = {
            let prepared = pending
                .as_mut()
                .ok_or(ClassicGroupExecutionError::Reconciliation)?;
            let supplemental = prepared.take_revocation_assignment();
            let matches = supplemental.as_ref().is_none_or(|assignment| {
                assignment == prepared.reconciliation().previous_assignment()
            });
            if let Some(assignment) = supplemental {
                prepared.restore_revocation_assignment(assignment);
            }
            matches
        };
        let prepared = pending
            .as_ref()
            .ok_or(ClassicGroupExecutionError::Reconciliation)?;
        let reconciliation = prepared.reconciliation();
        let previous = reconciliation.previous_assignment();
        let replacement = reconciliation.replacement_assignment();
        let split_authority_matches = prepared.position_was_installed()
            && supplemental_matches
            && prepared.membership_ownership_matches(owner.machine(), installed_rejoin)
            && catalog.live_assignment() == Some(previous)
            && catalog.membership_cycle() == Some(reconciliation.previous_cycle())
            && catalog.classic_generation()
                == Some(reconciliation.replacement_classic_generation().get())
            && previous.group_id() == replacement.group_id()
            && previous.member_id() == replacement.member_id();
        if !split_authority_matches {
            return Err(ClassicGroupExecutionError::Reconciliation);
        }

        let transition = owner
            .apply(ClassicGroupInput::Close)
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let first = effects.next();
        let second = effects.next();
        let (replacement_effect, replacement_generation) = match (first, second) {
            (
                Some(ClassicGroupEffect::Revoke {
                    assignment,
                    classic_generation,
                }),
                None,
            ) if &assignment == replacement
                && classic_generation == reconciliation.replacement_classic_generation() =>
            {
                (assignment, classic_generation)
            }
            _ => return Err(ClassicGroupExecutionError::UnexpectedCloseEffect),
        };

        let mut prepared = pending
            .take()
            .unwrap_or_else(|| unreachable!("validated reconciliation remains installed"));
        let supplemental = prepared.take_revocation_assignment();
        debug_assert!(prepared.take_position().is_none());
        let reconciliation = prepared.into_reconciliation();
        let (previous, owned_replacement, delta) = reconciliation.into_assignments();
        debug_assert_eq!(replacement_effect, owned_replacement);
        debug_assert!(
            supplemental
                .as_ref()
                .is_none_or(|assignment| assignment == &previous)
        );
        drop((replacement_effect, owned_replacement, delta, supplemental));

        match retire_and_revoke_classic_group_assignment(
            owner,
            catalog,
            processing_lease,
            fetch,
            previous,
            replacement_generation,
        ) {
            Ok(_retirement) => {}
            Err(failure) => {
                let kind = failure.kind;
                self.set_execution_state(ClassicGroupExecutionState::CloseFault {
                    revoke_failure: failure,
                });
                return Err(map_revocation_kind(kind));
            }
        }
        self.set_execution_state(ClassicGroupExecutionState::Idle);
        Ok(ClassicGroupCloseProgress::Progress)
    }

    pub(super) fn close_if_local(
        &mut self,
        owner: &mut ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
    ) -> Result<ClassicGroupCloseProgress, ClassicGroupExecutionError> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::JoinDriverOwned(driver_owned) => {
                return if owner.machine().group_id() == driver_owned.identity().group_id()
                    && owner.machine().active_cycle() == Some(driver_owned.identity().cycle())
                {
                    Ok(ClassicGroupCloseProgress::DriverOwned)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::PartitionCountDriverOwned { call, .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { call, .. } => {
                return if owner.machine().group_id() == call.identity().group_id()
                    && owner.machine().active_cycle() == Some(call.identity().cycle())
                {
                    Ok(ClassicGroupCloseProgress::DriverOwned)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::PartitionCountHandoff { .. }
            | ClassicGroupExecutionState::SyncHandoff(_) => {
                return Err(ClassicGroupExecutionError::HandoffIncomplete);
            }
            ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_) => {
                return Ok(ClassicGroupCloseProgress::DriverOwned);
            }
            ClassicGroupExecutionState::CloseFault { revoke_failure } => {
                return Err(map_revocation_kind(revoke_failure.kind));
            }
            ClassicGroupExecutionState::PartitionCountsPostCore { .. } => {
                return Err(ClassicGroupExecutionError::PartitionCountsPostCore);
            }
            ClassicGroupExecutionState::Idle
                if owner.machine().phase() == ClassicGroupPhase::Closed =>
            {
                return Ok(ClassicGroupCloseProgress::AlreadyClosed);
            }
            ClassicGroupExecutionState::Idle
            | ClassicGroupExecutionState::PreparedJoin(_)
            | ClassicGroupExecutionState::PreparedPartitionCounts(_)
            | ClassicGroupExecutionState::PreparedSync(_) => {}
        }
        let transition = owner
            .apply(ClassicGroupInput::Close)
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        match transition.into_effects().next() {
            None => {}
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }) => match retire_and_revoke_classic_group_assignment(
                owner,
                catalog,
                processing_lease,
                fetch,
                assignment,
                classic_generation,
            ) {
                Ok(_retirement) => {}
                Err(failure) => {
                    let kind = failure.kind;
                    self.set_execution_state(ClassicGroupExecutionState::CloseFault {
                        revoke_failure: failure,
                    });
                    return Err(map_revocation_kind(kind));
                }
            },
            Some(_) => return Err(ClassicGroupExecutionError::UnexpectedCloseEffect),
        }
        self.set_execution_state(ClassicGroupExecutionState::Idle);
        Ok(ClassicGroupCloseProgress::Progress)
    }

    pub(super) fn retry_close_fault(
        &mut self,
        owner: &ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::CloseFault { revoke_failure } = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::CloseNotFaulted);
        };
        match retire_and_revoke_classic_group_assignment(
            owner,
            catalog,
            processing_lease,
            fetch,
            revoke_failure.assignment,
            revoke_failure.classic_generation,
        ) {
            Ok(_retirement) => Ok(()),
            Err(failure) => {
                let kind = failure.kind;
                self.set_execution_state(ClassicGroupExecutionState::CloseFault {
                    revoke_failure: failure,
                });
                Err(map_revocation_kind(kind))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupCloseProgress {
    AlreadyClosed,
    Progress,
    DriverOwned,
}
