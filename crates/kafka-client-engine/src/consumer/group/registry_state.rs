//! Immediate observation of current driver-confirmed dynamic membership facts.

use std::sync::Arc;

use kafka_client_core::GroupId;

use crate::consumer::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerMetadata,
    GroupConsumerState, group_registration_request::GroupConsumerProtocol,
};

use super::{
    classic_group_fetch::current_consumer_group_position_fence, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState, registry_event::observable_classic_position_fence,
    registry_port::GroupConsumerPort, registry_shard::GroupConsumerShardLockError,
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
        let Some(current_assignment) = entry.catalog.live_assignment() else {
            return Ok(None);
        };
        let Some(current_member) = entry.catalog.current_member() else {
            return Err(GroupConsumerStateSnapshotError::EntryFault);
        };
        let Some(generation_id_or_member_epoch) =
            entry.catalog.classic_generation().or_else(|| {
                entry
                    .catalog
                    .consumer_group_member_epoch()
                    .map(kafka_client_core::ConsumerGroupMemberEpoch::get)
            })
        else {
            return Err(GroupConsumerStateSnapshotError::EntryFault);
        };
        if !entry.execution.is_idle() {
            return Ok(None);
        }
        let position_fence = match entry.protocol {
            GroupConsumerProtocol::Classic => match observable_classic_position_fence(entry)? {
                Some(fence) => fence,
                None => return Ok(None),
            },
            GroupConsumerProtocol::Consumer => current_consumer_group_position_fence(
                entry
                    .consumer
                    .as_ref()
                    .ok_or(GroupConsumerStateSnapshotError::EntryFault)?,
                &entry.catalog,
            )
            .map_err(|_error| GroupConsumerStateSnapshotError::EntryFault)?,
        };
        let epoch = current_assignment.assignment_generation().get();
        if !entry.catalog.events.is_confirmed(epoch) {
            return Ok(None);
        }
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(current_assignment.partitions().len())
            .map_err(|_error| GroupConsumerStateSnapshotError::Allocation)?;
        for assigned in current_assignment.partitions() {
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
                Arc::clone(current_member),
                match entry.protocol {
                    GroupConsumerProtocol::Classic => entry.catalog.group_instance_id().cloned(),
                    GroupConsumerProtocol::Consumer => None,
                },
                generation_id_or_member_epoch,
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
