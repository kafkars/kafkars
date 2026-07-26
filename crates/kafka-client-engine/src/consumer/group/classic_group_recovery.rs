//! Post-driver reconciliation of every bounded classic Join and Sync call.

use kafka_client_core::ClassicGroupInput;

use crate::driver::classic_group::{
    ClassicHeartbeatShutdownRecovery, JoinGroupShutdownRecovery,
    RecoveredClassicHeartbeatOwnership, RecoveredJoinGroupOwnership, RecoveredSyncGroupOwnership,
    SyncGroupShutdownRecovery,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_recovery::ClassicGroupSyncRecovery, registry::GroupConsumerRegistry,
};

enum SyncRecoveryFailure {
    Ownership(ClassicGroupExecutionError, RecoveredSyncGroupOwnership),
}

impl GroupConsumerRegistry {
    pub(super) fn recover_classic_calls_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ClassicGroupExecutionError> {
        self.recover_classic_group_positions_after_driver_shutdown()?;
        self.recover_classic_partition_counts_after_driver_shutdown()?;
        self.recover_classic_heartbeats_after_driver_shutdown()?;
        if self.sync_recovery_fault.is_some() || self.join_recovery_fault.is_some() {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        if self.sync_shutdown_recovery.is_none() {
            let calls = self
                .sync_calls
                .take()
                .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
            self.sync_shutdown_recovery = Some(calls.recover_sync_groups_after_driver_shutdown());
        }
        while let Some(recovered) = self.take_next_sync_recovery() {
            if let Err(failure) = self.reconcile_sync_recovery(recovered) {
                return match failure {
                    SyncRecoveryFailure::Ownership(error, recovered) => {
                        self.sync_recovery_fault = Some(recovered);
                        Err(error)
                    }
                };
            }
        }
        self.sync_shutdown_recovery = None;

        if self.join_shutdown_recovery.is_none() {
            let calls = self
                .join_calls
                .take()
                .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
            self.join_shutdown_recovery = Some(calls.recover_join_groups_after_driver_shutdown());
        }
        while let Some(recovered) = self.take_next_join_recovery() {
            if let Err((error, recovered)) = self.reconcile_join_recovery(recovered) {
                self.join_recovery_fault = Some(recovered);
                return Err(error);
            }
        }
        self.join_shutdown_recovery = None;
        self.recover_classic_coordinator_invalidations_after_driver_shutdown()
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed recovery returns the exact linear generated owner"
    )]
    fn reconcile_sync_recovery(
        &mut self,
        recovered: RecoveredSyncGroupOwnership,
    ) -> Result<(), SyncRecoveryFailure> {
        let group_id = recovered.key().group_id();
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(SyncRecoveryFailure::Ownership(
                ClassicGroupExecutionError::CallIdentityMismatch,
                recovered,
            ));
        };
        if entry.fault.is_some() {
            return Err(SyncRecoveryFailure::Ownership(
                ClassicGroupExecutionError::EntryFault,
                recovered,
            ));
        }
        let disposition = match entry
            .execution
            .inspect_sync_after_driver_shutdown(&recovered)
        {
            Ok(disposition) => disposition,
            Err(error) => return Err(SyncRecoveryFailure::Ownership(error, recovered)),
        };
        if let ClassicGroupSyncRecovery::BeforeSemantic(cycle) = disposition {
            let transition = match entry.classic.apply(ClassicGroupInput::SyncFailed { cycle }) {
                Ok(transition) => transition,
                Err(error) => {
                    entry.fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(cycle));
                    return Err(SyncRecoveryFailure::Ownership(
                        ClassicGroupExecutionError::Core(error.kind()),
                        recovered,
                    ));
                }
            };
            if transition.into_effects().next().is_some() {
                entry.fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(cycle));
                return Err(SyncRecoveryFailure::Ownership(
                    ClassicGroupExecutionError::SyncTerminal,
                    recovered,
                ));
            }
        }
        let _committed = entry
            .execution
            .reconcile_sync_after_driver_shutdown(recovered)
            .map_err(|(error, recovered)| SyncRecoveryFailure::Ownership(error, recovered))?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed recovery returns the exact linear generated owner"
    )]
    fn reconcile_join_recovery(
        &mut self,
        recovered: RecoveredJoinGroupOwnership,
    ) -> Result<(), (ClassicGroupExecutionError, RecoveredJoinGroupOwnership)> {
        let group_id = recovered.key().group_id();
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err((ClassicGroupExecutionError::CallIdentityMismatch, recovered));
        };
        if entry.fault.is_some() {
            return Err((ClassicGroupExecutionError::EntryFault, recovered));
        }
        entry
            .execution
            .reconcile_join_after_driver_shutdown(recovered)
    }

    fn take_next_sync_recovery(&mut self) -> Option<RecoveredSyncGroupOwnership> {
        let recovery = self.sync_shutdown_recovery.as_mut()?;
        recovery
            .pop_active()
            .or_else(|| recovery.take_settled())
            .or_else(|| recovery.take_pending())
            .or_else(|| recovery.take_completion())
    }

    fn take_next_join_recovery(&mut self) -> Option<RecoveredJoinGroupOwnership> {
        let recovery = self.join_shutdown_recovery.as_mut()?;
        recovery
            .pop_active()
            .or_else(|| recovery.take_settled())
            .or_else(|| recovery.take_pending())
            .or_else(|| recovery.take_completion())
    }
}

pub(super) fn recovery_unsettled_count(
    heartbeat: Option<&ClassicHeartbeatShutdownRecovery>,
    join: Option<&JoinGroupShutdownRecovery>,
    sync: Option<&SyncGroupShutdownRecovery>,
    heartbeat_fault: Option<&RecoveredClassicHeartbeatOwnership>,
    join_fault: Option<&RecoveredJoinGroupOwnership>,
    sync_fault: Option<&RecoveredSyncGroupOwnership>,
) -> usize {
    heartbeat
        .map_or(0, ClassicHeartbeatShutdownRecovery::retained_count)
        .saturating_add(join.map_or(0, JoinGroupShutdownRecovery::retained_count))
        .saturating_add(sync.map_or(0, SyncGroupShutdownRecovery::retained_count))
        .saturating_add(usize::from(heartbeat_fault.is_some()))
        .saturating_add(usize::from(join_fault.is_some()))
        .saturating_add(usize::from(sync_fault.is_some()))
}
