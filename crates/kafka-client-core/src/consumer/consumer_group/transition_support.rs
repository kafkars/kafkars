//! Shared identity validation and terminal ownership for KIP-848 transitions.

use crate::{LiveGroupAssignment, MemberId, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatFatal, ConsumerGroupHeartbeatMachine,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatSchedule, ConsumerGroupHeartbeatSequence,
    ConsumerGroupHeartbeatTransition, ConsumerGroupMemberEpoch,
};

impl ConsumerGroupHeartbeatMachine {
    pub(super) fn validate_in_flight(
        &self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
    ) -> Result<(), ConsumerGroupHeartbeatErrorKind> {
        if !matches!(
            self.phase,
            ConsumerGroupHeartbeatPhase::Joining | ConsumerGroupHeartbeatPhase::Heartbeating
        ) {
            return Err(ConsumerGroupHeartbeatErrorKind::InvalidPhase);
        }
        if self.in_flight != Some(attempt) {
            return Err(ConsumerGroupHeartbeatErrorKind::AttemptMismatch);
        }
        if self
            .deadline
            .is_none_or(|deadline| deadline.is_elapsed_at(now))
        {
            return Err(ConsumerGroupHeartbeatErrorKind::DeadlineElapsed);
        }
        Ok(())
    }

    pub(super) fn reserve_attempt(
        &self,
        member_epoch: Option<ConsumerGroupMemberEpoch>,
    ) -> Result<
        (
            ConsumerGroupHeartbeatAttempt,
            Option<ConsumerGroupHeartbeatSequence>,
        ),
        ConsumerGroupHeartbeatErrorKind,
    > {
        let sequence = self
            .next_sequence
            .ok_or(ConsumerGroupHeartbeatErrorKind::AttemptExhausted)?;
        Ok((
            ConsumerGroupHeartbeatAttempt::new(sequence, member_epoch),
            sequence.checked_next(),
        ))
    }

    pub(super) fn commit_success(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        assignment: LiveGroupAssignment,
        schedule: ConsumerGroupHeartbeatSchedule,
    ) {
        let generation = assignment.assignment_generation();
        self.phase = ConsumerGroupHeartbeatPhase::Stable;
        self.next_sequence = next_sequence;
        self.in_flight = None;
        self.deadline = None;
        self.member_id = Some(member_id);
        self.member_epoch = Some(member_epoch);
        self.next_assignment_generation = generation.checked_next();
        self.live_assignment = Some(assignment);
        self.schedule = Some(schedule);
    }

    pub(super) fn fail(
        &mut self,
        attempt: ConsumerGroupHeartbeatAttempt,
        failure: ConsumerGroupHeartbeatFailure,
    ) -> ConsumerGroupHeartbeatTransition {
        let assignment = self.live_assignment.take();
        let fatal = ConsumerGroupHeartbeatFatal::new(attempt, failure);
        self.phase = ConsumerGroupHeartbeatPhase::Fatal;
        self.clear_active();
        self.fatal = Some(fatal);
        match assignment {
            Some(assignment) => ConsumerGroupHeartbeatTransition::two(
                ConsumerGroupHeartbeatEffect::Revoke { assignment },
                ConsumerGroupHeartbeatEffect::Fatal { fatal },
            ),
            None => {
                ConsumerGroupHeartbeatTransition::one(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            }
        }
    }

    pub(super) fn clear_active(&mut self) {
        self.in_flight = None;
        self.deadline = None;
        self.member_id = None;
        self.member_epoch = None;
        self.schedule = None;
    }
}
