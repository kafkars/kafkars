//! Immediate observation of current driver-confirmed dynamic membership facts.

use std::sync::Arc;

use kafka_client_core::GroupId;

use crate::consumer::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerMetadata,
    GroupConsumerState,
};

use super::{
    classic_group_fetch::{ClassicGroupFetchCurrentFenceError, current_position_fence},
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerStateSnapshotError {
    UnknownGroup,
    Closing,
    EntryFault,
    Allocation,
    UnknownTopic,
}

impl GroupConsumerStateSnapshotError {
    pub(in crate::consumer) const fn is_allocation(self) -> bool {
        matches!(self, Self::Allocation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerStatePortError {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerStateSnapshotError),
}

impl GroupConsumerStatePortError {
    pub(in crate::consumer) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Registry(
                    GroupConsumerStateSnapshotError::UnknownGroup
                        | GroupConsumerStateSnapshotError::Closing
                        | GroupConsumerStateSnapshotError::EntryFault
                )
        )
    }
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn group_state(
        &self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerState>, GroupConsumerStateSnapshotError> {
        let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerStateSnapshotError::UnknownGroup);
        };
        if entry.state == GroupConsumerEntryState::Closing {
            return Err(GroupConsumerStateSnapshotError::Closing);
        }
        if entry.fault.is_some() {
            return Err(GroupConsumerStateSnapshotError::EntryFault);
        }
        let Some(current) = entry.catalog.current.as_ref() else {
            return Ok(None);
        };
        if !entry.execution.is_idle() {
            return Ok(None);
        }
        let position_fence = match current_position_fence(&entry.classic, &entry.catalog) {
            Ok(fence) => fence,
            Err(
                ClassicGroupFetchCurrentFenceError::MissingMembershipCycle
                | ClassicGroupFetchCurrentFenceError::MissingClassicAssignment
                | ClassicGroupFetchCurrentFenceError::MissingCatalogAssignment,
            ) => return Ok(None),
            Err(
                ClassicGroupFetchCurrentFenceError::CatalogGroupMismatch
                | ClassicGroupFetchCurrentFenceError::AssignmentMismatch,
            ) => return Err(GroupConsumerStateSnapshotError::EntryFault),
        };
        let epoch = current.assignment.assignment_generation().get();
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(current.assignment.partitions().len())
            .map_err(|_error| GroupConsumerStateSnapshotError::Allocation)?;
        for assigned in current.assignment.partitions() {
            let topic = entry
                .catalog
                .topic_name(assigned.topic_id())
                .map_err(|_error| GroupConsumerStateSnapshotError::UnknownTopic)?;
            let partition = i32::try_from(assigned.partition().get())
                .map_err(|_error| GroupConsumerStateSnapshotError::UnknownTopic)?;
            partitions.push(GroupConsumerAssignmentPartition::new(
                Arc::clone(topic),
                partition,
            ));
        }
        Ok(Some(GroupConsumerState::new(
            GroupConsumerAssignment::new(epoch, partitions),
            GroupConsumerMetadata::new_with_group_instance_id(
                Arc::clone(entry.catalog.group()),
                entry.catalog.group_instance_id().cloned(),
                Arc::clone(&current.member),
                current.classic_generation,
                epoch,
                position_fence,
            ),
        )))
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn try_group_state(
        &self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerState>, GroupConsumerStatePortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerStatePortError::Closed);
        }
        let registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerStatePortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerStatePortError::Closed);
        }
        registry
            .group_state(group_id)
            .map_err(GroupConsumerStatePortError::Registry)
    }
}
