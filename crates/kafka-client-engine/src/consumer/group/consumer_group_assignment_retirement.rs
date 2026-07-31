//! Ordered KIP-848 position, processing, Fetch, and catalog retirement.

use kafka_client_core::{
    ClassicGroupPhase, ClassicProcessingLeaseFence, ConsumerGroupHeartbeatPhase,
    LiveGroupAssignment, Moment,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_position::{ClassicGroupPositionCloseTurn, close_entry_position},
    consumer_group_execution::ConsumerGroupExecutionError,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupAssignmentRetirementTurn {
    Idle,
    Progress,
    Blocked,
}

pub(super) fn stage_consumer_group_revocation(
    entry: &mut GroupConsumerEntry,
    revoked: Option<LiveGroupAssignment>,
) -> Result<(), ConsumerGroupExecutionError> {
    let Some(assignment) = revoked else {
        return Ok(());
    };
    if entry.consumer_revocation.is_some() {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    match entry.catalog.live_assignment() {
        Some(current) if current == &assignment => {
            entry.consumer_revocation = Some(assignment);
            Ok(())
        }
        None if entry.position.is_dormant()
            && entry.processing_lease.active_schedule().is_none()
            && entry.processing_lease.pending_expiration().is_none()
            && entry.fetch.activation().is_none() =>
        {
            Ok(())
        }
        _ => Err(ConsumerGroupExecutionError::EffectShape),
    }
}

impl GroupConsumerRegistry {
    pub(super) fn turn_one_consumer_group_assignment_retirement(
        &mut self,
        now: Moment,
    ) -> Result<ConsumerGroupAssignmentRetirementTurn, ClassicGroupExecutionError> {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.consumer_revocation.is_some())
        else {
            return Ok(ConsumerGroupAssignmentRetirementTurn::Idle);
        };
        retire_entry_assignment(&mut self.entries[index], now)
    }
}

pub(super) fn retire_entry_assignment(
    entry: &mut GroupConsumerEntry,
    now: Moment,
) -> Result<ConsumerGroupAssignmentRetirementTurn, ClassicGroupExecutionError> {
    match close_entry_position(entry, now)? {
        ClassicGroupPositionCloseTurn::Progress => {
            return Ok(ConsumerGroupAssignmentRetirementTurn::Progress);
        }
        ClassicGroupPositionCloseTurn::Blocked => {
            return Ok(ConsumerGroupAssignmentRetirementTurn::Blocked);
        }
        ClassicGroupPositionCloseTurn::Idle => {}
    }
    let assignment = entry
        .consumer_revocation
        .as_ref()
        .ok_or(ClassicGroupExecutionError::ConsumerGroup)?;
    let cycle = entry
        .catalog
        .membership_cycle()
        .ok_or(ClassicGroupExecutionError::ConsumerGroup)?;
    let fence = ClassicProcessingLeaseFence::new(
        assignment.group_id(),
        cycle,
        assignment.assignment_generation(),
    );
    let processing = if entry.processing_lease.active_schedule().is_some()
        || entry.processing_lease.pending_expiration().is_some()
    {
        match entry.processing_lease.prepare_revocation(fence) {
            Ok(processing) => Some(processing),
            Err(error) => {
                entry.fault =
                    Some(ClassicGroupEntryFault::ConsumerGroupProcessingLeaseRevocation(error));
                return Err(ClassicGroupExecutionError::ProcessingLease(error));
            }
        }
    } else {
        None
    };
    let event = entry
        .catalog
        .prepare_assignment_retirement_event(assignment);
    let epoch = assignment.assignment_generation().get();
    if let Err(error) = entry.fetch.retire_for_assignment_loss(assignment) {
        drop(processing);
        entry.fault = Some(ClassicGroupEntryFault::ConsumerGroupFetchRetirement(error));
        return Err(ClassicGroupExecutionError::FetchRetirement(error));
    }
    if let Some(processing) = processing {
        let _transition = processing.commit();
    }
    let assignment = entry
        .consumer_revocation
        .take()
        .ok_or(ClassicGroupExecutionError::ConsumerGroup)?;
    entry.catalog.commit_consumer_group_revoke(assignment);
    let phase = entry
        .consumer
        .as_ref()
        .map(|consumer| consumer.machine().phase())
        .ok_or(ClassicGroupExecutionError::ConsumerGroup)?;
    entry.catalog.commit_assignment_retirement_event(
        event,
        epoch,
        if phase == ConsumerGroupHeartbeatPhase::Closed {
            ClassicGroupPhase::Closed
        } else {
            ClassicGroupPhase::Fatal
        },
    );
    Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
}
