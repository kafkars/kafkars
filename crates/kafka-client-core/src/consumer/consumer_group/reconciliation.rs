//! Heartbeat-safe KIP-848 target staging, retirement acknowledgement, and installation authority.

use crate::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, MemberId, Moment,
};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatSchedule, ConsumerGroupHeartbeatSequence,
    ConsumerGroupHeartbeatTransition, ConsumerGroupMemberEpoch,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn assignment_retired(
        &mut self,
        now: Moment,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment_generation: AssignmentGeneration,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase != ConsumerGroupHeartbeatPhase::Stable {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        let schedule = self
            .schedule
            .ok_or(ConsumerGroupHeartbeatErrorKind::ReconciliationMismatch)?;
        let current = self
            .live_assignment
            .as_ref()
            .ok_or(ConsumerGroupHeartbeatErrorKind::ReconciliationMismatch)?;
        let pending = self
            .pending_assignment
            .as_ref()
            .ok_or(ConsumerGroupHeartbeatErrorKind::ReconciliationMismatch)?;
        if self.member_id != Some(member_id)
            || self.member_epoch != Some(member_epoch)
            || current.group_id() != self.group_id
            || current.member_id() != member_id
            || current.assignment_generation() != assignment_generation
            || pending.group_id() != self.group_id
            || pending.member_id() != member_id
            || schedule.assignment_generation() != assignment_generation
            || schedule.attempt().member_epoch() != Some(member_epoch)
            || self.in_flight.is_some()
            || self.deadline.is_some()
            || self.retry_schedule.is_some()
        {
            return Err(ConsumerGroupHeartbeatErrorKind::ReconciliationMismatch);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let attempt = schedule.attempt();
        drop(self.live_assignment.take());
        self.phase = ConsumerGroupHeartbeatPhase::Heartbeating;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        self.rediscovery_replacement_used = false;
        self.retry_schedule = None;
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Steady,
                member_id: Some(member_id),
                member_epoch: Some(member_epoch),
                assignment_generation: None,
                deadline,
            },
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "initial assignment and its first cadence commit atomically"
    )]
    pub(super) fn install_initial_assignment(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        partitions: Vec<GroupAssignmentPartition>,
        next_attempt: ConsumerGroupHeartbeatAttempt,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let assignment_generation = self
            .next_assignment_generation
            .ok_or(ConsumerGroupHeartbeatErrorKind::AssignmentGenerationExhausted)?;
        let (retained, effect_assignment) = LiveGroupAssignment::try_new_pair(
            self.group_id,
            member_id,
            assignment_generation,
            partitions,
        )
        .map_err(|(error, _)| ConsumerGroupHeartbeatErrorKind::Assignment(error))?;
        let schedule = ConsumerGroupHeartbeatSchedule::new(
            next_attempt,
            cadence_deadline,
            assignment_generation,
        );
        self.commit_success(member_id, member_epoch, next_sequence, retained, schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Reconcile {
                previous: None,
                assignment: effect_assignment,
                member_epoch,
                schedule,
            },
        ))
    }

    pub(super) fn arm_reportable_assignment(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        next_attempt: ConsumerGroupHeartbeatAttempt,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        let assignment_generation = self
            .live_assignment
            .as_ref()
            .map(LiveGroupAssignment::assignment_generation)
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let schedule = ConsumerGroupHeartbeatSchedule::new(
            next_attempt,
            cadence_deadline,
            assignment_generation,
        );
        self.phase = ConsumerGroupHeartbeatPhase::Stable;
        self.next_sequence = next_sequence;
        self.in_flight = None;
        self.deadline = None;
        self.rediscovery_replacement_used = false;
        self.retry_schedule = None;
        self.member_id = Some(member_id);
        self.member_epoch = Some(member_epoch);
        self.schedule = Some(schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule },
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "acknowledgement validates the pending target and commits its cadence atomically"
    )]
    pub(super) fn settle_pending_reconciliation(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment: Option<&[GroupAssignmentPartition]>,
        next_attempt: ConsumerGroupHeartbeatAttempt,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.member_epoch != Some(member_epoch) {
            return Err(ConsumerGroupHeartbeatErrorKind::ReconciliationEpochChanged);
        }
        let pending = self
            .pending_assignment
            .as_ref()
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        if assignment.is_some_and(|partitions| partitions != pending.partitions()) {
            return Err(ConsumerGroupHeartbeatErrorKind::AssignmentChangedWithoutEpoch);
        }
        if self.live_assignment.is_some() {
            return self.arm_reportable_assignment(
                member_id,
                member_epoch,
                next_attempt,
                next_sequence,
                cadence_deadline,
            );
        }
        let target_generation = pending.assignment_generation();
        let target = self
            .pending_assignment
            .take()
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let schedule =
            ConsumerGroupHeartbeatSchedule::new(next_attempt, cadence_deadline, target_generation);
        self.commit_success(member_id, member_epoch, next_sequence, target, schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::InstallReconciled {
                member_id,
                member_epoch,
                assignment_generation: target_generation,
                schedule,
            },
        ))
    }
}
