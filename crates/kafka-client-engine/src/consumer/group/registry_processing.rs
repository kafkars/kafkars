//! Assignment-fenced processing-liveness scheduling for hosted classic groups.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput, ClassicGroupPhase,
    ClassicProcessingLeaseEffect, ClassicProcessingLeaseError, ClassicProcessingLeaseExpiration,
    ClassicProcessingLeaseInput, Deadline, Moment,
};

use super::{
    classic_group_assignment::{
        ClassicGroupRevocationFailure, ClassicGroupRevocationFailureKind,
        retire_and_revoke_classic_group_assignment,
    },
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_heartbeat::ClassicHeartbeatExecutionError,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

/// Result of observing at most one due application-processing lease.
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
            entry.fault.is_none() && entry.processing_lease.pending_expiration().is_some()
        }) {
            let expiration = self.entries[index]
                .processing_lease
                .pending_expiration()
                .ok_or(GroupConsumerProcessingError::UnexpectedLeaseEffect)?;
            apply_processing_assignment_loss(&mut self.entries[index], expiration)?;
            return Ok(GroupConsumerProcessingTurn::Progress);
        }
        let Some(index) = self.entries.iter().position(|entry| {
            entry.fault.is_none()
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
            .filter(|entry| entry.fault.is_none())
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
