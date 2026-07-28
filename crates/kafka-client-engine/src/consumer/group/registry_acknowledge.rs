//! Immediate assignment-fenced application-processing acknowledgment.

use kafka_client_core::{
    ClassicProcessingLeaseEffect, ClassicProcessingLeaseError, ClassicProcessingLeaseExpiration,
    ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, GroupId, GroupPositionFence, Moment,
};

use crate::clock::ClockError;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort, registry_shard::GroupConsumerShardLockError,
};

/// Private processing-acknowledgment failure before or after core progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerAcknowledgePortError {
    Closed,
    Clock(ClockError),
    Lock(GroupConsumerShardLockError),
    UnknownGroup,
    GroupUnavailable,
    StaleCheckpoint,
    Processing(ClassicProcessingLeaseError),
    Expired(ClassicProcessingLeaseExpiration),
    UnexpectedEffect,
}

impl GroupConsumerRegistry {
    /// Applies one checkpoint-fenced progress fact to the sole processing owner.
    pub(super) fn acknowledge_processing(
        &mut self,
        group_id: GroupId,
        checkpoint: GroupPositionFence,
        now: Moment,
    ) -> Result<(), GroupConsumerAcknowledgePortError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerAcknowledgePortError::UnknownGroup)?;
        if entry.state != GroupConsumerEntryState::Active
            || entry.fault.is_some()
            || !entry.revocation.is_dormant()
        {
            return Err(GroupConsumerAcknowledgePortError::GroupUnavailable);
        }
        let Some(assignment) = entry.catalog.live_assignment() else {
            return Err(GroupConsumerAcknowledgePortError::GroupUnavailable);
        };
        if checkpoint.group_id() != group_id
            || checkpoint.member_id() != assignment.member_id()
            || checkpoint.assignment_generation() != assignment.assignment_generation()
        {
            return Err(GroupConsumerAcknowledgePortError::StaleCheckpoint);
        }
        let fence = ClassicProcessingLeaseFence::new(
            checkpoint.group_id(),
            checkpoint.membership_cycle(),
            checkpoint.assignment_generation(),
        );
        let transition = entry
            .processing_lease
            .apply(ClassicProcessingLeaseInput::Progress { fence, now })
            .map_err(GroupConsumerAcknowledgePortError::Processing)?;
        let mut effects = transition.effects().copied();
        match (effects.next(), effects.next()) {
            (Some(ClassicProcessingLeaseEffect::Arm { schedule }), None)
                if schedule.fence() == fence =>
            {
                Ok(())
            }
            (Some(ClassicProcessingLeaseEffect::AssignmentLost { expiration }), None)
                if expiration.schedule().fence() == fence =>
            {
                Err(GroupConsumerAcknowledgePortError::Expired(expiration))
            }
            _ => Err(GroupConsumerAcknowledgePortError::UnexpectedEffect),
        }
    }
}

impl GroupConsumerPort {
    /// Captures progress time before contention and applies one exact checkpoint.
    pub(in crate::consumer) fn try_acknowledge_processing(
        &self,
        group_id: GroupId,
        checkpoint: GroupPositionFence,
    ) -> Result<(), GroupConsumerAcknowledgePortError> {
        let now = self
            .clock
            .now()
            .map_err(GroupConsumerAcknowledgePortError::Clock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerAcknowledgePortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerAcknowledgePortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerAcknowledgePortError::Closed);
        }
        let result = registry.acknowledge_processing(group_id, checkpoint, now);
        let processing_expired =
            matches!(result, Err(GroupConsumerAcknowledgePortError::Expired(_)));
        drop(registry);
        if processing_expired {
            let _wake = self.shared.request_turn();
        }
        result
    }
}
