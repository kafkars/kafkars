//! Sole state mutation paths for KIP-848 heartbeat and assignment ownership.

use crate::{Deadline, GroupAssignmentPartition, LiveGroupAssignment, MemberId, Moment};

use super::{
    CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS, ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind, ConsumerGroupHeartbeatFailure,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatSchedule, ConsumerGroupHeartbeatTransition, ConsumerGroupMemberEpoch,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn begin(
        &mut self,
        now: Moment,
        deadline: Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase == ConsumerGroupHeartbeatPhase::Closed {
            return Err(ConsumerGroupHeartbeatErrorKind::Closed);
        }
        if self.phase != ConsumerGroupHeartbeatPhase::Dormant {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        if self.member_id.is_some() || self.member_epoch.is_some() || self.live_assignment.is_some()
        {
            return Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation);
        }
        let (attempt, next_sequence) = self.reserve_attempt(None)?;
        self.phase = ConsumerGroupHeartbeatPhase::Joining;
        self.next_sequence = next_sequence;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Join,
                member_id: None,
                member_epoch: None,
                assignment_generation: None,
                deadline,
            },
        ))
    }

    pub(super) fn heartbeat_due(
        &mut self,
        schedule: ConsumerGroupHeartbeatSchedule,
        now: Moment,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if self.phase != ConsumerGroupHeartbeatPhase::Stable {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.schedule != Some(schedule) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        if !schedule.deadline().is_elapsed_at(now) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleNotDue);
        }
        let member_id = self
            .member_id
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let member_epoch = self
            .member_epoch
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        let assignment = self
            .live_assignment
            .as_ref()
            .ok_or(ConsumerGroupHeartbeatErrorKind::InvariantViolation)?;
        if assignment.assignment_generation() != schedule.assignment_generation() {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        let deadline = now
            .checked_deadline_after(self.policy.attempt_timeout_ticks())
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let attempt = schedule.attempt();
        if attempt.member_epoch() != Some(member_epoch) {
            return Err(ConsumerGroupHeartbeatErrorKind::ScheduleMismatch);
        }
        self.phase = ConsumerGroupHeartbeatPhase::Heartbeating;
        self.schedule = None;
        self.in_flight = Some(attempt);
        self.deadline = Some(deadline);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::Submit {
                group_id: self.group_id,
                attempt,
                kind: ConsumerGroupHeartbeatRequestKind::Steady,
                member_id: Some(member_id),
                member_epoch: Some(member_epoch),
                assignment_generation: Some(assignment.assignment_generation()),
                deadline,
            },
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one normalized broker response carries every fenced KIP-848 scalar"
    )]
    pub(super) fn heartbeat_succeeded(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        heartbeat_interval_ticks: u64,
        throttle_ticks: u64,
        assignment: Option<Vec<GroupAssignmentPartition>>,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        self.validate_in_flight(attempt, now)?;
        if self.member_id.is_some_and(|current| current != member_id) {
            return Err(ConsumerGroupHeartbeatErrorKind::MemberMismatch);
        }
        if heartbeat_interval_ticks == 0 {
            return Err(ConsumerGroupHeartbeatErrorKind::ZeroHeartbeatInterval);
        }
        let prior_epoch = self.member_epoch;
        if prior_epoch.is_some_and(|current| member_epoch < current) {
            return Err(ConsumerGroupHeartbeatErrorKind::MemberEpochRegression);
        }
        if prior_epoch.is_none() && assignment.is_none() {
            return Err(ConsumerGroupHeartbeatErrorKind::InitialAssignmentMissing);
        }
        if prior_epoch.is_some_and(|current| member_epoch > current) && assignment.is_none() {
            return Err(ConsumerGroupHeartbeatErrorKind::ChangedEpochMissingAssignment);
        }
        if let Some(partitions) = assignment.as_ref() {
            if partitions.len() > CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS {
                return Err(ConsumerGroupHeartbeatErrorKind::AssignmentTooLarge);
            }
            if prior_epoch == Some(member_epoch)
                && self
                    .live_assignment
                    .as_ref()
                    .is_some_and(|live| live.partitions() != partitions)
            {
                return Err(ConsumerGroupHeartbeatErrorKind::AssignmentChangedWithoutEpoch);
            }
        }
        let cadence_ticks = heartbeat_interval_ticks.max(throttle_ticks);
        let cadence_deadline = now
            .checked_deadline_after(cadence_ticks)
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let (next_attempt, next_sequence) = self.reserve_attempt(Some(member_epoch))?;

        if let Some(partitions) = assignment {
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
            let previous = self.live_assignment.take();
            self.commit_success(member_id, member_epoch, next_sequence, retained, schedule);
            return Ok(ConsumerGroupHeartbeatTransition::one(
                ConsumerGroupHeartbeatEffect::Reconcile {
                    previous,
                    assignment: effect_assignment,
                    member_epoch,
                    schedule,
                },
            ));
        }

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
        self.member_id = Some(member_id);
        self.member_epoch = Some(member_epoch);
        self.schedule = Some(schedule);
        Ok(ConsumerGroupHeartbeatTransition::one(
            ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule },
        ))
    }

    pub(super) fn heartbeat_failed(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Joining | ConsumerGroupHeartbeatPhase::Heartbeating
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        Ok(self.fail(attempt, failure))
    }
}
