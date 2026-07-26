//! Explicit coordinator-invalidation disposal after embedded driver shutdown.

use kafka_client_core::GroupId;

use super::coordinator_invalidation::{
    ClassicCoordinatorInvalidationState, ClassicCoordinatorInvalidations,
};

/// Linear ownership of invalidations recovered only after the driver is gone.
#[must_use = "recovered invalidations must be explicitly discarded after driver shutdown"]
pub(crate) struct ClassicCoordinatorInvalidationShutdownRecovery {
    entries: Vec<ClassicCoordinatorInvalidationState>,
}

impl ClassicCoordinatorInvalidationShutdownRecovery {
    pub(crate) fn discard_one_after_driver_shutdown(&mut self) -> Option<GroupId> {
        let entry = self.entries.pop()?;
        let group_id = entry.group_id();
        drop(entry);
        Some(group_id)
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn storage_capacity_for_test(&self) -> usize {
        self.entries.capacity()
    }
}

impl ClassicCoordinatorInvalidations {
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> ClassicCoordinatorInvalidationShutdownRecovery {
        ClassicCoordinatorInvalidationShutdownRecovery {
            entries: self.entries,
        }
    }
}
