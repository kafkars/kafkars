//! Successful KIP-848 heartbeat routing across stable and reconciling ownership.

use crate::{GroupAssignmentPartition, MemberId, Moment};

use super::{
    CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS, ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatErrorKind, ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatSequence,
    ConsumerGroupHeartbeatTransition, ConsumerGroupMemberEpoch,
};

impl ConsumerGroupHeartbeatMachine {
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
        self.validate_success_identity(attempt, now, member_id, member_epoch)?;
        if heartbeat_interval_ticks == 0 {
            return Err(ConsumerGroupHeartbeatErrorKind::ZeroHeartbeatInterval);
        }
        if assignment
            .as_ref()
            .is_some_and(|partitions| partitions.len() > CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS)
        {
            return Err(ConsumerGroupHeartbeatErrorKind::AssignmentTooLarge);
        }
        let cadence_deadline = now
            .checked_deadline_after(heartbeat_interval_ticks.max(throttle_ticks))
            .ok_or(ConsumerGroupHeartbeatErrorKind::DeadlineOverflow)?;
        let (next_attempt, next_sequence) = self.reserve_attempt(Some(member_epoch))?;

        let transition = if self.pending_assignment.is_some() {
            self.settle_pending_reconciliation(
                member_id,
                member_epoch,
                assignment.as_deref(),
                next_attempt,
                next_sequence,
                cadence_deadline,
            )
        } else {
            self.settle_without_pending_target(
                member_id,
                member_epoch,
                assignment,
                next_attempt,
                next_sequence,
                cadence_deadline,
            )
        }?;
        self.initial_heartbeat_succeeded = true;
        Ok(transition)
    }

    fn settle_without_pending_target(
        &mut self,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
        assignment: Option<Vec<GroupAssignmentPartition>>,
        next_attempt: ConsumerGroupHeartbeatAttempt,
        next_sequence: Option<ConsumerGroupHeartbeatSequence>,
        cadence_deadline: crate::Deadline,
    ) -> Result<ConsumerGroupHeartbeatTransition, ConsumerGroupHeartbeatErrorKind> {
        match (self.member_epoch, self.live_assignment.as_ref(), assignment) {
            (None, None, Some(partitions)) => self.install_initial_assignment(
                member_id,
                member_epoch,
                partitions,
                next_attempt,
                next_sequence,
                cadence_deadline,
            ),
            (None, None, None) => self.await_initial_assignment(
                member_id,
                member_epoch,
                next_attempt,
                next_sequence,
                cadence_deadline,
            ),
            (Some(prior), None, Some(partitions)) if member_epoch >= prior => self
                .install_initial_assignment(
                    member_id,
                    member_epoch,
                    partitions,
                    next_attempt,
                    next_sequence,
                    cadence_deadline,
                ),
            (Some(prior), None, None) if member_epoch >= prior => self.await_initial_assignment(
                member_id,
                member_epoch,
                next_attempt,
                next_sequence,
                cadence_deadline,
            ),
            (Some(prior), Some(live), Some(partitions)) if member_epoch == prior => {
                if live.partitions() != partitions {
                    return Err(ConsumerGroupHeartbeatErrorKind::AssignmentChangedWithoutEpoch);
                }
                self.arm_reportable_assignment(
                    member_id,
                    member_epoch,
                    next_attempt,
                    next_sequence,
                    cadence_deadline,
                )
            }
            (Some(prior), Some(_), None) if member_epoch == prior => self
                .arm_reportable_assignment(
                    member_id,
                    member_epoch,
                    next_attempt,
                    next_sequence,
                    cadence_deadline,
                ),
            (Some(prior), Some(live), Some(partitions)) if member_epoch > prior => {
                if live.partitions() == partitions {
                    return self.arm_reportable_assignment(
                        member_id,
                        member_epoch,
                        next_attempt,
                        next_sequence,
                        cadence_deadline,
                    );
                }
                self.stage_replacement(
                    member_id,
                    member_epoch,
                    partitions,
                    next_attempt,
                    next_sequence,
                    cadence_deadline,
                )
            }
            (Some(prior), Some(_), None) if member_epoch > prior => {
                Err(ConsumerGroupHeartbeatErrorKind::ChangedEpochMissingAssignment)
            }
            _ => Err(ConsumerGroupHeartbeatErrorKind::InvariantViolation),
        }
    }

    fn validate_success_identity(
        &self,
        attempt: ConsumerGroupHeartbeatAttempt,
        now: Moment,
        member_id: MemberId,
        member_epoch: ConsumerGroupMemberEpoch,
    ) -> Result<(), ConsumerGroupHeartbeatErrorKind> {
        self.validate_in_flight(attempt, now)?;
        if self.member_id.is_some_and(|current| current != member_id) {
            return Err(ConsumerGroupHeartbeatErrorKind::MemberMismatch);
        }
        if self
            .member_epoch
            .is_some_and(|current| member_epoch < current)
        {
            return Err(ConsumerGroupHeartbeatErrorKind::MemberEpochRegression);
        }
        Ok(())
    }
}
