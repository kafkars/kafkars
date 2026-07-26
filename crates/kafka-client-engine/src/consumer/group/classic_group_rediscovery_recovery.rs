//! Explicit rediscovery-capability disposal after the unique driver owner is gone.

use crate::driver::classic_group::ClassicCoordinatorInvalidationShutdownRecovery;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry,
};

impl GroupConsumerRegistry {
    pub(super) fn recover_classic_coordinator_invalidations_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ClassicGroupExecutionError> {
        if self.coordinator_invalidation_shutdown_recovery.is_none() {
            let invalidations = self
                .coordinator_invalidations
                .take()
                .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
            self.coordinator_invalidation_shutdown_recovery =
                Some(invalidations.recover_after_driver_shutdown());
        }
        while let Some(group_id) = self
            .coordinator_invalidation_shutdown_recovery
            .as_mut()
            .and_then(
                ClassicCoordinatorInvalidationShutdownRecovery::discard_one_after_driver_shutdown,
            )
        {
            if !self
                .entries
                .iter()
                .any(|entry| entry.group_id() == group_id)
            {
                return Err(ClassicGroupExecutionError::CallIdentityMismatch);
            }
        }
        self.coordinator_invalidation_shutdown_recovery = None;

        for entry in &mut self.entries {
            let Some(fault) = entry.fault.take() else {
                entry.rediscovery.clear_rediscovery_after_driver_shutdown();
                continue;
            };
            match fault {
                ClassicGroupEntryFault::CoordinatorInvalidationInstall(failure)
                    if failure.expected_group_id() == entry.group_id()
                        && failure.pending_group_id() == entry.group_id() =>
                {
                    let discarded_group = failure.discard_after_driver_shutdown();
                    if discarded_group != entry.group_id() {
                        return Err(ClassicGroupExecutionError::CallIdentityMismatch);
                    }
                }
                fault => {
                    entry.fault = Some(fault);
                }
            }
            entry.rediscovery.clear_rediscovery_after_driver_shutdown();
        }
        Ok(())
    }
}
