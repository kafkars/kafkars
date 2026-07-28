//! Exact removal of one never-started classic-group registration.

use kafka_client_core::GroupId;

use super::{
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

/// Stable reason one registration could not be removed without group protocol work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerDormantUnregisterError {
    RegistryClosing,
    UnknownGroup,
    GroupClosing,
    EntryFault,
    NotDormant,
    RetainedBytesInvariant,
}

impl GroupConsumerRegistry {
    /// Removes only the exact pristine entry created by registration.
    ///
    /// Once any membership cycle or other per-group mechanism has started,
    /// ordinary close and `LeaveGroup` policy remain the only release path.
    pub(super) fn try_unregister_dormant(
        &mut self,
        group_id: GroupId,
    ) -> Result<(), GroupConsumerDormantUnregisterError> {
        if !self.accepting {
            return Err(GroupConsumerDormantUnregisterError::RegistryClosing);
        }
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerDormantUnregisterError::UnknownGroup);
        };
        let entry = &self.entries[index];
        if !matches!(entry.state, GroupConsumerEntryState::Active) {
            return Err(GroupConsumerDormantUnregisterError::GroupClosing);
        }
        if entry.fault.is_some() {
            return Err(GroupConsumerDormantUnregisterError::EntryFault);
        }
        if !is_pristine_registration(entry) {
            return Err(GroupConsumerDormantUnregisterError::NotDormant);
        }
        let Some(retained_group_bytes) = self.retained_group_bytes.checked_sub(entry.group_bytes())
        else {
            return Err(GroupConsumerDormantUnregisterError::RetainedBytesInvariant);
        };

        let removed = self.entries.remove(index);
        debug_assert_eq!(removed.group_id(), group_id);
        self.retained_group_bytes = retained_group_bytes;
        Ok(())
    }
}

fn is_pristine_registration(entry: &GroupConsumerEntry) -> bool {
    entry.catalog.live_assignment().is_none()
        && entry.classic.is_dormant()
        && entry.classic.pending().is_none()
        && entry.execution.is_idle()
        && entry.heartbeat.is_dormant()
        && entry.position.is_dormant()
        && entry.processing_lease.active_schedule().is_none()
        && entry.processing_lease.pending_expiration().is_none()
        && entry.rejoin.is_dormant()
        && !entry.rediscovery.blocks_join()
        && entry.fetch.is_idle()
}
