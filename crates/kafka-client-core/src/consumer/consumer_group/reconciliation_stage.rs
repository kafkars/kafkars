//! Bounded duplicate ownership for one staged KIP-848 replacement target.

use crate::{GroupAssignmentPartition, LiveGroupAssignment, LiveGroupAssignmentError, MemberId};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatSchedule,
    ConsumerGroupHeartbeatSequence, ConsumerGroupHeartbeatTransition, ConsumerGroupMemberEpoch,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn stage_replacement(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        partitions: Vec<GroupAssignmentPartition>,
        next_attempt: ConsumerGroupHeartbeatAttempt,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let target_generation = self
            .next_assignment_generation
            .ok_or(ConsumerGroupHeartbeatErrorKind::AssignmentGenerationExhausted)?;
        let (target, effect_target) = LiveGroupAssignment::try_new_pair(
            self.group_id,
            member_id,
            target_generation,
            partitions,
        )
        .map_err(|(error, _)| ConsumerGroupHeartbeatErrorKind::Assignment(error))?;
        let current = self
            .live_assignment
            .as_ref()
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let effect_current = duplicate_assignment(current)?;
        let schedule = ConsumerGroupHeartbeatSchedule::new(
            next_attempt,
            cadence_deadline,
            current.assignment_generation(),
        );
        self.phase = ConsumerGroupHeartbeatPhase::Stable;
        self.next_sequence = next_sequence;
        self.in_flight = None;
        self.deadline = None;
        self.rediscovery_replacement_used = false;
        self.retry_schedule = None;
        self.member_id = Some(member_id);
        self.member_epoch = Some(member_epoch);
        self.next_assignment_generation = target_generation.checked_next();
        self.pending_assignment = Some(target);
        self.schedule = Some(schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Reconcile {
                previous: Some(effect_current),
                assignment: effect_target,
                member_epoch,
                schedule,
            },
        ))
    }
}

fn duplicate_assignment(
    assignment: &LiveGroupAssignment,
) -> Result<LiveGroupAssignment, ConsumerGroupHeartbeatErrorKind> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(assignment.partitions().len())
        .map_err(|_error| {
            ConsumerGroupHeartbeatErrorKind::Assignment(LiveGroupAssignmentError::AllocationFailed)
        })?;
    partitions.extend_from_slice(assignment.partitions());
    LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        partitions,
    )
    .map_err(ConsumerGroupHeartbeatErrorKind::Assignment)
}
