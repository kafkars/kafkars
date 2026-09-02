//! Deadline-captured admission and terminal probing for one group seek.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignedTopicPartition, GroupAssignmentPartition, GroupId, GroupPositionFence, PartitionIndex,
    StartPosition,
};

use crate::clock::{ClockError, DeadlineCapture};
use crate::consumer::group_control::GroupConsumerPartition;
use crate::consumer::group_recv::{GroupConsumerRecvRegistration, GroupConsumerRecvWait};
use crate::consumer::group_seek::GroupConsumerSeekCompletion;

use super::{
    classic_group_fetch::{ClassicGroupFetchSeekError, ClassicGroupFetchSeekObservation},
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    registry_wake::GroupConsumerShardWakeError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerSeekRegistryError {
    UnknownGroup,
    GroupUnavailable,
    NoAssignment,
    UnknownPartition,
    Pending,
    ResourceExhausted,
    HostUnavailable,
    InternalInvariant,
}

impl GroupConsumerRegistry {
    pub(super) fn seek_partition(
        &mut self,
        group_id: GroupId,
        partition: GroupConsumerPartition,
        position: StartPosition,
        capture: DeadlineCapture,
        completion: Arc<GroupConsumerSeekCompletion>,
    ) -> Result<(), GroupConsumerSeekRegistryError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerSeekRegistryError::UnknownGroup)?;
        if entry.state != GroupConsumerEntryState::Active || !entry.revocation.is_dormant() {
            return Err(GroupConsumerSeekRegistryError::GroupUnavailable);
        }
        if entry.fault.is_some() || entry.fetch.is_faulted_for_seek() {
            return Err(GroupConsumerSeekRegistryError::HostUnavailable);
        }
        let assignment = entry
            .catalog
            .live_assignment()
            .ok_or(GroupConsumerSeekRegistryError::NoAssignment)?;
        let cycle = if entry.uses_consumer_group_protocol() {
            entry.catalog.membership_cycle()
        } else {
            entry.classic.machine().active_cycle()
        }
        .ok_or(GroupConsumerSeekRegistryError::NoAssignment)?;
        let topic_id = entry
            .catalog
            .topic_id(partition.topic())
            .ok_or(GroupConsumerSeekRegistryError::UnknownPartition)?;
        let partition_index = PartitionIndex::from_raw(partition.partition().cast_unsigned());
        if !assignment
            .partitions()
            .contains(&GroupAssignmentPartition::new(topic_id, partition_index))
        {
            return Err(GroupConsumerSeekRegistryError::UnknownPartition);
        }
        let position_fence = GroupPositionFence::new(
            group_id,
            cycle,
            assignment.member_id(),
            assignment.assignment_generation(),
        );
        drop(partition);
        entry
            .fetch
            .seek_partition(
                position_fence,
                AssignedTopicPartition::new(topic_id, partition_index),
                position,
                capture,
                completion,
            )
            .map_err(translate_fetch_seek)
    }

    fn observe_seek(
        &self,
        group_id: GroupId,
        completion: &Arc<GroupConsumerSeekCompletion>,
    ) -> GroupConsumerSeekObservation {
        let Some(entry) = self.entry(group_id) else {
            return GroupConsumerSeekObservation::HostUnavailable;
        };
        if entry.state != GroupConsumerEntryState::Active
            || entry.fault.is_some()
            || entry.fetch.is_faulted_for_seek()
        {
            return GroupConsumerSeekObservation::HostUnavailable;
        }
        match entry.fetch.seek_observation(completion) {
            ClassicGroupFetchSeekObservation::Pending => GroupConsumerSeekObservation::Pending,
            ClassicGroupFetchSeekObservation::TerminalMissing => {
                GroupConsumerSeekObservation::TerminalMissing
            }
        }
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn capture_seek_deadline(
        &self,
        timeout: Duration,
    ) -> Result<DeadlineCapture, GroupConsumerSeekPortError> {
        self.clock
            .capture_deadline_after(timeout)
            .map_err(GroupConsumerSeekPortError::Clock)
    }

    pub(in crate::consumer) fn try_seek_captured(
        &self,
        group_id: GroupId,
        partition: GroupConsumerPartition,
        position: StartPosition,
        capture: DeadlineCapture,
    ) -> Result<GroupConsumerSeekAdmission, GroupConsumerSeekPortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerSeekPortError::Closed);
        }
        let completion = Arc::new(GroupConsumerSeekCompletion::pending());
        let registration = self
            .arm_group_recv_blocking(group_id, None, GroupConsumerRecvWait::Unlock)
            .map_err(|_error| GroupConsumerSeekPortError::Notification)?;
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                self.cancel_group_recv(&mut Some(registration));
                return Err(GroupConsumerSeekPortError::Lock(error));
            }
        };
        if self.shared.admission_is_closed() {
            drop(registry);
            self.cancel_group_recv(&mut Some(registration));
            return Err(GroupConsumerSeekPortError::Closed);
        }
        if let Err(error) = registry.seek_partition(
            group_id,
            partition,
            position,
            capture,
            Arc::clone(&completion),
        ) {
            drop(registry);
            self.cancel_group_recv(&mut Some(registration));
            return Err(GroupConsumerSeekPortError::Registry(error));
        }
        drop(registry);
        Ok(GroupConsumerSeekAdmission {
            completion,
            registration,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(in crate::consumer) fn observe_seek(
        &self,
        group_id: GroupId,
        completion: &Arc<GroupConsumerSeekCompletion>,
    ) -> Result<GroupConsumerSeekObservation, GroupConsumerSeekObservationError> {
        let registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(GroupConsumerShardLockError::Contended) => {
                return Ok(GroupConsumerSeekObservation::Pending);
            }
            Err(error) => return Err(GroupConsumerSeekObservationError::Lock(error)),
        };
        Ok(registry.observe_seek(group_id, completion))
    }
}

#[must_use = "accepted seek retains its bounded notification registration"]
pub(in crate::consumer) struct GroupConsumerSeekAdmission {
    pub(in crate::consumer) completion: Arc<GroupConsumerSeekCompletion>,
    pub(in crate::consumer) registration: GroupConsumerRecvRegistration,
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerSeekAdmission {
    pub(in crate::consumer) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerSeekPortError {
    Closed,
    Clock(ClockError),
    Notification,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerSeekRegistryError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerSeekObservation {
    Pending,
    TerminalMissing,
    HostUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerSeekObservationError {
    Lock(GroupConsumerShardLockError),
}

const fn translate_fetch_seek(error: ClassicGroupFetchSeekError) -> GroupConsumerSeekRegistryError {
    match error {
        ClassicGroupFetchSeekError::Faulted => GroupConsumerSeekRegistryError::HostUnavailable,
        ClassicGroupFetchSeekError::Inactive | ClassicGroupFetchSeekError::BindingMismatch => {
            GroupConsumerSeekRegistryError::NoAssignment
        }
        ClassicGroupFetchSeekError::Pending => GroupConsumerSeekRegistryError::Pending,
        ClassicGroupFetchSeekError::Capacity => GroupConsumerSeekRegistryError::ResourceExhausted,
        ClassicGroupFetchSeekError::UnknownPartition => {
            GroupConsumerSeekRegistryError::UnknownPartition
        }
        ClassicGroupFetchSeekError::Core => GroupConsumerSeekRegistryError::InternalInvariant,
    }
}
