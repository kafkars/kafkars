//! Assignment-fenced processing-liveness scheduling for hosted classic groups.
use super::{
    classic_group_assignment::{
        ClassicGroupRevocationFailure, ClassicGroupRevocationFailureKind,
        retire_and_revoke_classic_group_assignment,
    },
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_heartbeat::ClassicHeartbeatExecutionError,
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_execution::ConsumerGroupExecutionError,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};
use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput, ClassicGroupPhase,
    ClassicProcessingLeaseEffect, ClassicProcessingLeaseError, ClassicProcessingLeaseExpiration,
    ClassicProcessingLeaseInput, Deadline, Moment,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerProcessingTurn {
    Idle,
    Progress,
}

/// Stable processing-liveness failure with exact ownership retained by the entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerProcessingError {
    Lease(ClassicProcessingLeaseError),
    Membership(ClassicGroupErrorKind),
    UnexpectedLeaseEffect,
    UnexpectedMembershipEffect,
    AssignmentFenceMismatch,
    Revocation(ClassicGroupRevocationFailureKind),
    Heartbeat(ClassicHeartbeatExecutionError),
    ConsumerGroup(ConsumerGroupExecutionError),
}

impl GroupConsumerRegistry {
    pub(super) fn entry_fault_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.fault.is_some())
            .count()
    }

    /// Applies one exact due processing deadline before membership can heartbeat.
    pub(super) fn turn_processing(
        &mut self,
        now: Moment,
    ) -> Result<GroupConsumerProcessingTurn, GroupConsumerProcessingError> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry.fault.is_none()
                && entry.classic_reconciliation.is_none()
                && entry.revocation.is_dormant()
                && entry.processing_lease.pending_expiration().is_some()
        }) {
            if self.entries[index].uses_consumer_group_protocol()
                && self.entries[index]
                    .consumer
                    .as_ref()
                    .is_some_and(|consumer| consumer.heartbeat_call().is_some())
            {
                return Ok(GroupConsumerProcessingTurn::Idle);
            }
            let expiration = self.entries[index]
                .processing_lease
                .pending_expiration()
                .ok_or(GroupConsumerProcessingError::UnexpectedLeaseEffect)?;
            apply_processing_assignment_loss(&mut self.entries[index], expiration)?;
            return Ok(GroupConsumerProcessingTurn::Progress);
        }
        let Some(index) = self.entries.iter().position(|entry| {
            entry.fault.is_none()
                && entry.classic_reconciliation.is_none()
                && entry.revocation.is_dormant()
                && entry
                    .processing_lease
                    .active_schedule()
                    .is_some_and(|schedule| schedule.deadline().is_elapsed_at(now))
        }) else {
            return Ok(GroupConsumerProcessingTurn::Idle);
        };
        expire_processing_lease(&mut self.entries[index], now)?;
        Ok(GroupConsumerProcessingTurn::Progress)
    }

    /// Returns the earliest exact application-processing deadline.
    pub(super) fn processing_next_deadline(&self) -> Option<Deadline> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.fault.is_none()
                    && entry.classic_reconciliation.is_none()
                    && entry.revocation.is_dormant()
            })
            .filter_map(|entry| entry.processing_lease.next_deadline())
            .min()
    }

    /// Counts every armed or expired processing owner independently.
    pub(super) fn processing_unsettled(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                entry.processing_lease.active_schedule().is_some()
                    || entry.processing_lease.pending_expiration().is_some()
            })
            .count()
    }
}

fn expire_processing_lease(
    entry: &mut GroupConsumerEntry,
    now: Moment,
) -> Result<(), GroupConsumerProcessingError> {
    let schedule = entry
        .processing_lease
        .active_schedule()
        .ok_or(GroupConsumerProcessingError::UnexpectedLeaseEffect)?;
    let transition = entry
        .processing_lease
        .apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence: schedule.fence(),
            now,
        })
        .map_err(GroupConsumerProcessingError::Lease)?;
    let mut effects = transition.effects().copied();
    let expiration = match (effects.next(), effects.next()) {
        (Some(ClassicProcessingLeaseEffect::AssignmentLost { expiration }), None)
            if expiration.schedule() == schedule =>
        {
            expiration
        }
        _ => return Err(GroupConsumerProcessingError::UnexpectedLeaseEffect),
    };
    apply_processing_assignment_loss(entry, expiration)
}

fn apply_processing_assignment_loss(
    entry: &mut GroupConsumerEntry,
    expiration: ClassicProcessingLeaseExpiration,
) -> Result<(), GroupConsumerProcessingError> {
    if entry.uses_consumer_group_protocol() {
        return apply_consumer_group_processing_loss(entry, expiration);
    }
    let fence = expiration.schedule().fence();
    if entry.group_id() != fence.group_id()
        || entry.classic.machine().active_cycle() != Some(fence.cycle())
        || entry.catalog.live_assignment().is_none_or(|assignment| {
            assignment.group_id() != fence.group_id()
                || assignment.assignment_generation() != fence.assignment_generation()
        })
    {
        entry.fault = Some(ClassicGroupEntryFault::ProcessingSemantic(expiration));
        return Err(GroupConsumerProcessingError::AssignmentFenceMismatch);
    }
    let transition = match entry.classic.apply(ClassicGroupInput::AssignmentLost {
        cycle: fence.cycle(),
    }) {
        Ok(transition) => transition,
        Err(error) => {
            let kind = error.kind();
            entry.fault = Some(ClassicGroupEntryFault::ProcessingSemantic(expiration));
            return Err(GroupConsumerProcessingError::Membership(kind));
        }
    };
    let mut effects = transition.into_effects();
    let (assignment, classic_generation) = match (effects.next(), effects.next()) {
        (
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            None,
        ) if assignment.group_id() == fence.group_id()
            && assignment.assignment_generation() == fence.assignment_generation() =>
        {
            (assignment, classic_generation)
        }
        (first, second) => {
            entry.fault = Some(ClassicGroupEntryFault::ProcessingPostCore {
                expiration,
                first,
                second,
            });
            return Err(GroupConsumerProcessingError::UnexpectedMembershipEffect);
        }
    };
    match retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        classic_generation,
    ) {
        Ok(_retirement) => {}
        Err(failure) => return retain_revocation_failure(entry, expiration, failure),
    }
    if !entry.heartbeat.blocks_close() {
        entry
            .heartbeat
            .clear_local()
            .map_err(GroupConsumerProcessingError::Heartbeat)?;
    }
    debug_assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    debug_assert!(entry.catalog.live_assignment().is_none());
    debug_assert!(entry.processing_lease.active_schedule().is_none());
    debug_assert!(entry.processing_lease.pending_expiration().is_none());
    Ok(())
}

fn apply_consumer_group_processing_loss(
    entry: &mut GroupConsumerEntry,
    expiration: ClassicProcessingLeaseExpiration,
) -> Result<(), GroupConsumerProcessingError> {
    let fence = expiration.schedule().fence();
    let assignment_matches = entry.catalog.live_assignment().is_some_and(|assignment| {
        assignment.group_id() == fence.group_id()
            && assignment.assignment_generation() == fence.assignment_generation()
    });
    let consumer_matches = entry.consumer.as_ref().is_some_and(|consumer| {
        consumer.cycle() == Some(fence.cycle())
            && consumer.machine().live_assignment() == entry.catalog.live_assignment()
    });
    if entry.group_id() != fence.group_id() || !assignment_matches || !consumer_matches {
        entry.fault = Some(ClassicGroupEntryFault::ProcessingSemantic(expiration));
        return Err(GroupConsumerProcessingError::AssignmentFenceMismatch);
    }
    let revoked = entry
        .consumer
        .as_mut()
        .ok_or(GroupConsumerProcessingError::AssignmentFenceMismatch)?
        .close_locally()
        .map_err(GroupConsumerProcessingError::ConsumerGroup)?;
    drop(entry.consumer_reconciliation.take());
    stage_consumer_group_revocation(entry, revoked)
        .map_err(GroupConsumerProcessingError::ConsumerGroup)?;
    Ok(())
}

fn retain_revocation_failure(
    entry: &mut GroupConsumerEntry,
    expiration: ClassicProcessingLeaseExpiration,
    failure: ClassicGroupRevocationFailure,
) -> Result<(), GroupConsumerProcessingError> {
    let kind = failure.kind;
    entry.fault = Some(ClassicGroupEntryFault::ProcessingRevoke {
        expiration,
        failure,
    });
    Err(GroupConsumerProcessingError::Revocation(kind))
}
