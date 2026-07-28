//! Current-assignment translation and one-wake admission for group Fetch control.

use kafka_client_core::{
    AssignedTopicPartition, GroupAssignmentPartition, GroupId, GroupPositionFence, PartitionIndex,
};

use crate::{
    clock::{ClockError, DeadlineCapture},
    consumer::group_control::GroupConsumerPartition,
};

use super::{
    classic_group_fetch::{ClassicGroupFetchControlAccepted, ClassicGroupFetchControlError},
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
};

/// Immediate control rejection before deterministic progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerControlPortError {
    Closed,
    Lock(GroupConsumerShardLockError),
    UnknownGroup,
    GroupUnavailable,
    NoAssignment,
    DuplicatePartition,
    UnknownPartition,
    Allocation,
    Fetch(ClassicGroupFetchControlError),
}

/// Accepted core progress plus advisory host degradation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) struct GroupConsumerControlPortAccepted {
    retained_invariant: bool,
    wake_failed: bool,
}

impl GroupConsumerControlPortAccepted {
    const fn new(retained_invariant: bool, wake_failed: bool) -> Self {
        Self {
            retained_invariant,
            wake_failed,
        }
    }

    pub(in crate::consumer) const fn retained_invariant(self) -> bool {
        self.retained_invariant
    }

    pub(in crate::consumer) const fn wake_failed(self) -> bool {
        self.wake_failed
    }
}

impl GroupConsumerRegistry {
    pub(super) fn pause_partitions(
        &mut self,
        group_id: GroupId,
        partitions: &[GroupConsumerPartition],
    ) -> Result<ClassicGroupFetchControlAccepted, GroupConsumerControlPortError> {
        let entry = self.control_entry(group_id)?;
        let (position_fence, targets) = prepare_control(entry, group_id, partitions)?;
        entry
            .fetch
            .pause_partitions(position_fence, &targets)
            .map_err(GroupConsumerControlPortError::Fetch)
    }

    pub(super) fn resume_partitions(
        &mut self,
        group_id: GroupId,
        partitions: &[GroupConsumerPartition],
        capture: DeadlineCapture,
    ) -> Result<ClassicGroupFetchControlAccepted, GroupConsumerControlPortError> {
        let entry = self.control_entry(group_id)?;
        let (position_fence, targets) = prepare_control(entry, group_id, partitions)?;
        entry
            .fetch
            .resume_partitions(position_fence, &targets, capture)
            .map_err(GroupConsumerControlPortError::Fetch)
    }

    fn control_entry(
        &mut self,
        group_id: GroupId,
    ) -> Result<&mut super::registry_entry::GroupConsumerEntry, GroupConsumerControlPortError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerControlPortError::UnknownGroup)?;
        if entry.state != GroupConsumerEntryState::Active
            || entry.fault.is_some()
            || !entry.revocation.is_dormant()
        {
            return Err(GroupConsumerControlPortError::GroupUnavailable);
        }
        Ok(entry)
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn try_pause_partitions(
        &self,
        group_id: GroupId,
        partitions: &[GroupConsumerPartition],
    ) -> Result<GroupConsumerControlPortAccepted, GroupConsumerControlPortError> {
        self.try_control(group_id, partitions, None)
    }

    pub(in crate::consumer) fn capture_group_resume_deadline(
        &self,
        timeout: std::time::Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        self.clock.capture_deadline_after(timeout)
    }

    pub(in crate::consumer) fn try_resume_partitions_captured(
        &self,
        group_id: GroupId,
        partitions: &[GroupConsumerPartition],
        capture: DeadlineCapture,
    ) -> Result<GroupConsumerControlPortAccepted, GroupConsumerControlPortError> {
        self.try_control(group_id, partitions, Some(capture))
    }

    fn try_control(
        &self,
        group_id: GroupId,
        partitions: &[GroupConsumerPartition],
        resume_at: Option<DeadlineCapture>,
    ) -> Result<GroupConsumerControlPortAccepted, GroupConsumerControlPortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerControlPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerControlPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerControlPortError::Closed);
        }
        let result = match resume_at {
            Some(capture) => registry.resume_partitions(group_id, partitions, capture),
            None => registry.pause_partitions(group_id, partitions),
        };
        let accepted = result?;
        let effects = accepted.effects();
        let retained_invariant = accepted.fault_retained();
        drop(registry);
        let wake_failed =
            (effects != 0 || retained_invariant) && self.shared.request_turn().is_err();
        Ok(GroupConsumerControlPortAccepted::new(
            retained_invariant,
            wake_failed,
        ))
    }
}

fn prepare_control(
    entry: &super::registry_entry::GroupConsumerEntry,
    group_id: GroupId,
    partitions: &[GroupConsumerPartition],
) -> Result<(GroupPositionFence, Vec<AssignedTopicPartition>), GroupConsumerControlPortError> {
    ensure_unique(partitions)?;
    let assignment = entry
        .catalog
        .live_assignment()
        .ok_or(GroupConsumerControlPortError::NoAssignment)?;
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .ok_or(GroupConsumerControlPortError::NoAssignment)?;
    let position_fence = GroupPositionFence::new(
        group_id,
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(partitions.len())
        .map_err(|_error| GroupConsumerControlPortError::Allocation)?;
    for partition in partitions {
        let topic_id = entry
            .catalog
            .topic_id(partition.topic())
            .ok_or(GroupConsumerControlPortError::UnknownPartition)?;
        let partition_index = PartitionIndex::from_raw(partition.partition().cast_unsigned());
        let assigned = GroupAssignmentPartition::new(topic_id, partition_index);
        if !assignment.partitions().contains(&assigned) {
            return Err(GroupConsumerControlPortError::UnknownPartition);
        }
        targets.push(AssignedTopicPartition::new(topic_id, partition_index));
    }
    Ok((position_fence, targets))
}

fn ensure_unique(
    partitions: &[GroupConsumerPartition],
) -> Result<(), GroupConsumerControlPortError> {
    for (index, partition) in partitions.iter().enumerate() {
        if partitions[..index].iter().any(|present| {
            present.topic() == partition.topic() && present.partition() == partition.partition()
        }) {
            return Err(GroupConsumerControlPortError::DuplicatePartition);
        }
    }
    Ok(())
}
