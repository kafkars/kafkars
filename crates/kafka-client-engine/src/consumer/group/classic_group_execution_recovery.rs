//! Exact post-driver reconciliation of membership receipts and execution state.

use crate::driver::classic_group::{
    JoinGroupCallKey, RecoveredJoinGroupOwnership, RecoveredSyncGroupOwnership, SyncGroupCallKey,
};

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_call::ClassicGroupJoinCallOwner,
    classic_group_sync::ClassicGroupSyncDriverOwner,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupSyncRecovery {
    BeforeSemantic(kafka_client_core::MembershipCycle),
    AfterSemantic,
}

#[expect(
    clippy::large_enum_variant,
    reason = "recovery retains the exact linear successor without another allocation"
)]
enum JoinRecoveryState {
    DriverOwned,
    Confirmation(ClassicGroupJoinSuccessor),
}

impl ClassicGroupExecution {
    pub(super) fn inspect_sync_after_driver_shutdown(
        &self,
        recovered: &RecoveredSyncGroupOwnership,
    ) -> Result<ClassicGroupSyncRecovery, ClassicGroupExecutionError> {
        let (owner, disposition) = match self.borrow_execution_state() {
            ClassicGroupExecutionState::SyncDriverOwned(owner) => (
                owner,
                ClassicGroupSyncRecovery::BeforeSemantic(owner.identity().cycle()),
            ),
            ClassicGroupExecutionState::SyncConfirmationPending(owner) => {
                (owner, ClassicGroupSyncRecovery::AfterSemantic)
            }
            _ => return Err(ClassicGroupExecutionError::HandoffMismatch),
        };
        let identity = owner.identity();
        let expected_key =
            SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        if owner.accepted().key() != recovered.key() || owner.accepted().key() != expected_key {
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        }
        Ok(disposition)
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed recovery returns the exact linear generated owner"
    )]
    pub(super) fn reconcile_join_after_driver_shutdown(
        &mut self,
        recovered: RecoveredJoinGroupOwnership,
    ) -> Result<(), (ClassicGroupExecutionError, RecoveredJoinGroupOwnership)> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let (call, recovery_state) = match state {
            ClassicGroupExecutionState::JoinDriverOwned(call) => {
                (call, JoinRecoveryState::DriverOwned)
            }
            ClassicGroupExecutionState::JoinConfirmationPending { call, successor } => {
                (call, JoinRecoveryState::Confirmation(successor))
            }
            state => {
                self.set_execution_state(state);
                return Err((ClassicGroupExecutionError::HandoffMismatch, recovered));
            }
        };
        let (integration, tracking, accepted) = call.into_parts();
        let identity = integration.identity();
        let expected_key =
            JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        let exact = tracking.identity() == identity
            && accepted.key() == recovered.key()
            && accepted.key() == expected_key;
        if !exact {
            self.restore_join_recovery_state(integration, tracking, accepted, recovery_state);
            return Err((ClassicGroupExecutionError::HandoffMismatch, recovered));
        }
        match recovered.reconcile_join_group_after_driver_shutdown(accepted) {
            Ok(()) => {
                self.set_execution_state(match recovery_state {
                    JoinRecoveryState::Confirmation(ClassicGroupJoinSuccessor::Idle) => {
                        ClassicGroupExecutionState::Idle
                    }
                    JoinRecoveryState::Confirmation(ClassicGroupJoinSuccessor::Join(prepared)) => {
                        ClassicGroupExecutionState::PreparedJoin(prepared)
                    }
                    JoinRecoveryState::Confirmation(
                        ClassicGroupJoinSuccessor::PartitionCounts(prepared),
                    ) => ClassicGroupExecutionState::PreparedPartitionCounts(prepared),
                    JoinRecoveryState::Confirmation(ClassicGroupJoinSuccessor::Sync(prepared)) => {
                        ClassicGroupExecutionState::PreparedSync(prepared)
                    }
                    JoinRecoveryState::DriverOwned => {
                        ClassicGroupExecutionState::PreparedJoin(integration.into_prepared())
                    }
                });
                Ok(())
            }
            Err(failure) => {
                let (accepted, recovered, _error) = failure.into_parts();
                self.restore_join_recovery_state(integration, tracking, accepted, recovery_state);
                Err((ClassicGroupExecutionError::HandoffMismatch, recovered))
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed recovery returns the exact linear generated owner"
    )]
    pub(super) fn reconcile_sync_after_driver_shutdown(
        &mut self,
        recovered: RecoveredSyncGroupOwnership,
    ) -> Result<ClassicGroupSyncRecovery, (ClassicGroupExecutionError, RecoveredSyncGroupOwnership)>
    {
        let disposition = match self.inspect_sync_after_driver_shutdown(&recovered) {
            Ok(disposition) => disposition,
            Err(error) => return Err((error, recovered)),
        };
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let (owner, confirming) = match state {
            ClassicGroupExecutionState::SyncDriverOwned(owner) => (owner, false),
            ClassicGroupExecutionState::SyncConfirmationPending(owner) => (owner, true),
            state => {
                self.set_execution_state(state);
                return Err((ClassicGroupExecutionError::HandoffMismatch, recovered));
            }
        };
        let (identity, accepted) = owner.into_parts();
        let expected_key =
            SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        let exact = accepted.key() == recovered.key() && accepted.key() == expected_key;
        if !exact {
            self.restore_sync_recovery_state(identity, accepted, confirming);
            return Err((ClassicGroupExecutionError::HandoffMismatch, recovered));
        }
        match recovered.reconcile_sync_group_after_driver_shutdown(accepted) {
            Ok(()) => Ok(disposition),
            Err(failure) => {
                let (accepted, recovered, _error) = failure.into_parts();
                self.restore_sync_recovery_state(identity, accepted, confirming);
                Err((ClassicGroupExecutionError::HandoffMismatch, recovered))
            }
        }
    }

    fn restore_join_recovery_state(
        &mut self,
        integration: super::classic_group_join::ClassicGroupJoinIntegrationOwner,
        tracking: super::classic_group_join::ClassicGroupJoinTracking,
        accepted: crate::driver::classic_group::AcceptedJoinGroupCall,
        recovery_state: JoinRecoveryState,
    ) {
        let call = ClassicGroupJoinCallOwner::new(integration, tracking, accepted);
        self.set_execution_state(match recovery_state {
            JoinRecoveryState::Confirmation(successor) => {
                ClassicGroupExecutionState::JoinConfirmationPending { call, successor }
            }
            JoinRecoveryState::DriverOwned => ClassicGroupExecutionState::JoinDriverOwned(call),
        });
    }

    fn restore_sync_recovery_state(
        &mut self,
        identity: super::classic_group_sync::ClassicGroupSyncIdentity,
        accepted: crate::driver::classic_group::AcceptedSyncGroupCall,
        confirming: bool,
    ) {
        let owner = ClassicGroupSyncDriverOwner::new(identity, accepted);
        self.set_execution_state(if confirming {
            ClassicGroupExecutionState::SyncConfirmationPending(owner)
        } else {
            ClassicGroupExecutionState::SyncDriverOwned(owner)
        });
    }
}
