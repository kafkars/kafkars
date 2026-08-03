//! Immediate group-selected observation of retained assignment transitions.

use kafka_client_core::{GroupId, GroupPositionFence};

use crate::consumer::GroupConsumerEvent;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort, registry_shard::GroupConsumerShardLockError,
};

use super::registry_entry::GroupConsumerEntry;
pub(super) use super::registry_state::GroupConsumerStateSnapshotError as GroupConsumerStateError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerEventError {
    UnknownGroup,
    Closing,
    EntryFault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerEventPortError {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerEventError),
}

impl GroupConsumerEventPortError {
    pub(in crate::consumer) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Registry(
                    GroupConsumerEventError::UnknownGroup
                        | GroupConsumerEventError::Closing
                        | GroupConsumerEventError::EntryFault
                )
        )
    }

    pub(in crate::consumer) const fn is_contended(self) -> bool {
        matches!(self, Self::Lock(GroupConsumerShardLockError::Contended))
    }

    pub(in crate::consumer) const fn is_host_unavailable(self) -> bool {
        matches!(self, Self::Lock(GroupConsumerShardLockError::Poisoned))
    }
}

/// Resolves the checkpoint fence visible beside one confirmed classic assignment.
///
/// Cooperative membership deliberately has two bounded split-authority windows:
/// a retained Join/Sync keeps the prior assignment while the active attempt uses
/// a newer cycle, and confirmed reconciliation advances core ownership before
/// the catalog can physically replace the prior assignment. This observer
/// validates those states without weakening Fetch activation's strict equality
/// fence.
pub(super) fn observable_classic_position_fence(
    entry: &GroupConsumerEntry,
) -> Result<Option<GroupPositionFence>, GroupConsumerStateError> {
    let Some(catalog_assignment) = entry.catalog.live_assignment() else {
        return Ok(None);
    };
    let catalog_cycle = entry
        .catalog
        .membership_cycle()
        .ok_or(GroupConsumerStateError::EntryFault)?;
    let catalog_generation = entry
        .catalog
        .classic_generation()
        .ok_or(GroupConsumerStateError::EntryFault)?;
    let catalog_identity_matches = catalog_assignment.group_id() == entry.group_id()
        && catalog_assignment.group_id() == entry.catalog.group_id()
        && entry.catalog.current_member_id() == Some(catalog_assignment.member_id());
    if !catalog_identity_matches {
        return Err(GroupConsumerStateError::EntryFault);
    }

    if let Some(pending) = entry.classic_reconciliation.as_ref() {
        if !pending.sync_is_confirmed() {
            return Ok(None);
        }
        let reconciliation = pending.reconciliation();
        let previous = reconciliation.previous_assignment();
        let replacement = reconciliation.replacement_assignment();
        let split_matches = pending
            .membership_ownership_matches(entry.classic.machine(), entry.rejoin.schedule())
            && catalog_assignment == previous
            && catalog_cycle == reconciliation.previous_cycle()
            && catalog_generation == reconciliation.replacement_classic_generation().get()
            && previous.member_id() == replacement.member_id()
            && previous.group_id() == replacement.group_id();
        if !split_matches {
            return Err(GroupConsumerStateError::EntryFault);
        }
        return Ok(Some(GroupPositionFence::new(
            previous.group_id(),
            reconciliation.previous_cycle(),
            previous.member_id(),
            previous.assignment_generation(),
        )));
    }

    let current_matches = entry.classic.machine().live_assignment() == Some(catalog_assignment)
        && entry.classic.machine().live_cycle() == Some(catalog_cycle)
        && entry
            .classic
            .machine()
            .live_generation()
            .map(kafka_client_core::ClassicGeneration::get)
            == Some(catalog_generation);
    if !current_matches {
        return Err(GroupConsumerStateError::EntryFault);
    }
    Ok(Some(GroupPositionFence::new(
        catalog_assignment.group_id(),
        catalog_cycle,
        catalog_assignment.member_id(),
        catalog_assignment.assignment_generation(),
    )))
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn take_event(
        &mut self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerEvent>, GroupConsumerEventError> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerEventError::UnknownGroup);
        };
        if let Some(event) = entry.catalog.take_event() {
            return Ok(Some(event));
        }
        if entry.state == GroupConsumerEntryState::Closing {
            return Err(GroupConsumerEventError::Closing);
        }
        if entry.fault.is_some() {
            return Err(GroupConsumerEventError::EntryFault);
        }
        Ok(None)
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn try_take_event(
        &self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerEvent>, GroupConsumerEventPortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerEventPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerEventPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerEventPortError::Closed);
        }
        registry
            .take_event(group_id)
            .map_err(GroupConsumerEventPortError::Registry)
    }
}
