//! Successful share heartbeat assignment and cadence transitions.

use crate::{GroupAssignmentPartition, LiveGroupAssignment, Moment};

use super::{
    SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS, ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect,
    ShareGroupHeartbeatErrorKind, ShareGroupHeartbeatMachine, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatSchedule, ShareGroupHeartbeatTransition, ShareGroupMemberEpoch,
};

impl ShareGroupHeartbeatMachine {
    pub(super) fn heartbeat_succeeded(
        &mut self,
        attempt: ShareGroupHeartbeatAttempt,
        now: Moment,
        member_epoch: ShareGroupMemberEpoch,
        heartbeat_interval_ticks: u64,
        throttle_ticks: u64,
        assignment: Option<Vec<GroupAssignmentPartition>>,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        self.validate_in_flight(attempt, now)?;
        if self
            .member_epoch
            .is_some_and(|current| member_epoch < current)
        {
            return Err(ShareGroupHeartbeatErrorKind::MemberEpochRegression);
        }
        if heartbeat_interval_ticks == 0 {
            return Err(ShareGroupHeartbeatErrorKind::ZeroHeartbeatInterval);
        }
        if assignment
            .as_ref()
            .is_some_and(|partitions| partitions.len() > SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS)
        {
            return Err(ShareGroupHeartbeatErrorKind::AssignmentTooLarge);
        }
        let cadence_deadline = now
            .checked_deadline_after(heartbeat_interval_ticks.max(throttle_ticks))
            .ok_or(ShareGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let (next_attempt, next_sequence) = self.reserve_attempt(Some(member_epoch))?;

        let transition = match assignment {
            Some(partitions)
                if self
                    .live_assignment
                    .as_ref()
                    .is_some_and(|live| live.partitions() == partitions) =>
            {
                let generation = self
                    .live_assignment
                    .as_ref()
                    .map(LiveGroupAssignment::assignment_generation);
                let schedule =
                    ShareGroupHeartbeatSchedule::new(next_attempt, cadence_deadline, generation);
                self.commit_cadence(
                    ShareGroupHeartbeatPhase::Stable,
                    member_epoch,
                    next_sequence,
                    schedule,
                );
                ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::ArmHeartbeat {
                    schedule,
                })
            }
            Some(partitions) => self.replace_assignment(
                member_epoch,
                partitions,
                next_attempt,
                next_sequence,
                cadence_deadline,
            )?,
            None if self.live_assignment.is_some() => {
                let generation = self
                    .live_assignment
                    .as_ref()
                    .map(LiveGroupAssignment::assignment_generation);
                let schedule =
                    ShareGroupHeartbeatSchedule::new(next_attempt, cadence_deadline, generation);
                self.commit_cadence(
                    ShareGroupHeartbeatPhase::Stable,
                    member_epoch,
                    next_sequence,
                    schedule,
                );
                ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::ArmHeartbeat {
                    schedule,
                })
            }
            None => {
                let schedule =
                    ShareGroupHeartbeatSchedule::new(next_attempt, cadence_deadline, None);
                self.commit_cadence(
                    ShareGroupHeartbeatPhase::AwaitingAssignment,
                    member_epoch,
                    next_sequence,
                    schedule,
                );
                ShareGroupHeartbeatTransition::one(ShareGroupHeartbeatEffect::AwaitAssignment {
                    member_epoch,
                    schedule,
                })
            }
        };
        self.initial_heartbeat_succeeded = true;
        Ok(transition)
    }

    fn replace_assignment(
        &mut self,
        member_epoch: ShareGroupMemberEpoch,
        partitions: Vec<GroupAssignmentPartition>,
        next_attempt: ShareGroupHeartbeatAttempt,
        next_sequence: Option<super::ShareGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ShareGroupHeartbeatTransition, ShareGroupHeartbeatErrorKind> {
        let generation = self
            .next_assignment_generation
            .ok_or(ShareGroupHeartbeatErrorKind::AssignmentGenerationExhausted)?;
        let (retained, effect) = LiveGroupAssignment::try_new_pair(
            self.group_id,
            self.member_id,
            generation,
            partitions,
        )
        .map_err(|(error, _partitions)| ShareGroupHeartbeatErrorKind::Assignment(error))?;
        let schedule =
            ShareGroupHeartbeatSchedule::new(next_attempt, cadence_deadline, Some(generation));
        let previous = self.live_assignment.replace(retained);
        self.next_assignment_generation = generation.checked_next();
        self.commit_cadence(
            ShareGroupHeartbeatPhase::Stable,
            member_epoch,
            next_sequence,
            schedule,
        );
        Ok(ShareGroupHeartbeatTransition::one(
            ShareGroupHeartbeatEffect::ReplaceAssignment {
                previous,
                assignment: effect,
                member_epoch,
                schedule,
            },
        ))
    }

    fn commit_cadence(
        &mut self,
        phase: ShareGroupHeartbeatPhase,
        member_epoch: ShareGroupMemberEpoch,
        next_sequence: Option<super::ShareGroupHeartbeatSequence>,
        schedule: ShareGroupHeartbeatSchedule,
    ) {
        self.phase = phase;
        self.next_sequence = next_sequence;
        self.in_flight = None;
        self.deadline = None;
        self.retry_schedule = None;
        self.member_epoch = Some(member_epoch);
        self.schedule = Some(schedule);
    }
}
