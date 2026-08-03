//! Stage-aware broker rejection and coordinator-loss recovery transitions.

use crate::{AssignmentGeneration, Moment};

use super::super::transition_support::validate_stage_cycle;
use super::{
    ClassicBrokerError, ClassicBrokerStage, ClassicGroupEffect, ClassicGroupErrorKind,
    ClassicGroupFatal, ClassicGroupFatalReason, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTransition, ClassicHeartbeatAttempt, ClassicProtocol, ClassicRejoinSchedule,
    MembershipCycle,
    error_disposition::{ClassicErrorDisposition, disposition, is_rebalance_in_progress},
};

impl ClassicGroupMachine {
    pub(in crate::consumer::classic_group) fn join_rejected(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        error: ClassicBrokerError,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_stage_cycle(self, ClassicGroupPhase::Joining, cycle)?;
        if self.stage_deadline_is_elapsed(now)? {
            return self.deadline_elapsed(cycle, now);
        }
        let assignment_generation = self
            .live_assignment()
            .map(crate::LiveGroupAssignment::assignment_generation);
        self.stage_rejected(
            ClassicBrokerStage::Join,
            cycle,
            assignment_generation,
            now,
            error,
        )
    }

    pub(in crate::consumer::classic_group) fn sync_rejected(
        &mut self,
        cycle: MembershipCycle,
        now: Moment,
        error: ClassicBrokerError,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        validate_stage_cycle(self, ClassicGroupPhase::Syncing, cycle)?;
        if self.stage_deadline_is_elapsed(now)? {
            return self.deadline_elapsed(cycle, now);
        }
        let assignment_generation = self
            .live_assignment()
            .map(crate::LiveGroupAssignment::assignment_generation);
        self.stage_rejected(
            ClassicBrokerStage::Sync,
            cycle,
            assignment_generation,
            now,
            error,
        )
    }

    pub(in crate::consumer::classic_group) fn heartbeat_rejected(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
        error: ClassicBrokerError,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        if self.heartbeat.attempt_deadline_is_elapsed(attempt, now)? {
            return self.heartbeat_deadline_elapsed(attempt, now);
        }
        self.heartbeat.failed(attempt)?;
        self.stage_rejected(
            ClassicBrokerStage::Heartbeat,
            attempt.cycle(),
            Some(attempt.assignment_generation()),
            now,
            error,
        )
    }

    pub(in crate::consumer::classic_group) fn heartbeat_coordinator_lost(
        &mut self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        self.validate_heartbeat_assignment(attempt)?;
        if self.heartbeat.attempt_deadline_is_elapsed(attempt, now)? {
            return self.heartbeat_deadline_elapsed(attempt, now);
        }
        let followup = self.coordinator_loss_followup(attempt, now);
        self.heartbeat.failed(attempt)?;
        let revoke = self.take_stable_revoke()?;
        Ok(match followup {
            RejectionFollowup::Rejoin {
                schedule,
                coordinator,
            } => {
                self.wait_to_rejoin(schedule);
                ClassicGroupTransition::two(
                    revoke,
                    ClassicGroupEffect::ArmRejoin {
                        schedule,
                        coordinator,
                    },
                )
            }
            RejectionFollowup::Fatal(fatal) => {
                self.retain_fatal(fatal);
                ClassicGroupTransition::two(revoke, ClassicGroupEffect::Fatal { fatal })
            }
        })
    }

    fn stage_deadline_is_elapsed(&self, now: Moment) -> Result<bool, ClassicGroupErrorKind> {
        let deadline = self
            .deadline()
            .ok_or(ClassicGroupErrorKind::InvariantViolation)?;
        Ok(deadline.is_elapsed_at(now))
    }

    fn stage_rejected(
        &mut self,
        stage: ClassicBrokerStage,
        cycle: MembershipCycle,
        assignment_generation: Option<AssignmentGeneration>,
        now: Moment,
        error: ClassicBrokerError,
    ) -> Result<ClassicGroupTransition, ClassicGroupErrorKind> {
        let retain_ownership = self.protocol() == ClassicProtocol::CooperativeSticky
            && is_rebalance_in_progress(error);
        let finish_reconciliation_before_rejoin =
            retain_ownership && self.phase == ClassicGroupPhase::Reconciling;
        match self.rejection_followup(stage, cycle, assignment_generation, now, error) {
            RejectionFollowup::Rejoin {
                schedule,
                coordinator,
            } => {
                let revoke = if self.live_assignment.is_some() && !retain_ownership {
                    Some(self.take_stable_revoke()?)
                } else {
                    None
                };
                if finish_reconciliation_before_rejoin {
                    self.wait_to_rejoin_after_reconciliation(schedule);
                } else {
                    self.wait_to_rejoin(schedule);
                }
                let arm = ClassicGroupEffect::ArmRejoin {
                    schedule,
                    coordinator,
                };
                Ok(match revoke {
                    Some(revoke) => ClassicGroupTransition::two(revoke, arm),
                    None => ClassicGroupTransition::one(arm),
                })
            }
            RejectionFollowup::Fatal(fatal) => self.finish_fatal(fatal),
        }
    }

    fn rejection_followup(
        &self,
        stage: ClassicBrokerStage,
        cycle: MembershipCycle,
        assignment_generation: Option<AssignmentGeneration>,
        now: Moment,
        error: ClassicBrokerError,
    ) -> RejectionFollowup {
        match disposition(stage, error) {
            ClassicErrorDisposition::Fatal => RejectionFollowup::Fatal(ClassicGroupFatal::new(
                cycle,
                assignment_generation,
                ClassicGroupFatalReason::Broker { stage, error },
            )),
            ClassicErrorDisposition::Rejoin(coordinator) => {
                let Some(due) = now.checked_deadline_after(self.rejoin_policy().backoff_ticks())
                else {
                    return RejectionFollowup::Fatal(ClassicGroupFatal::new(
                        cycle,
                        assignment_generation,
                        ClassicGroupFatalReason::ScheduleDeadlineOverflow,
                    ));
                };
                RejectionFollowup::Rejoin {
                    schedule: ClassicRejoinSchedule::new(cycle, assignment_generation, due),
                    coordinator,
                }
            }
        }
    }

    fn coordinator_loss_followup(
        &self,
        attempt: ClassicHeartbeatAttempt,
        now: Moment,
    ) -> RejectionFollowup {
        let Some(due) = now.checked_deadline_after(self.rejoin_policy().backoff_ticks()) else {
            return RejectionFollowup::Fatal(ClassicGroupFatal::new(
                attempt.cycle(),
                Some(attempt.assignment_generation()),
                ClassicGroupFatalReason::ScheduleDeadlineOverflow,
            ));
        };
        RejectionFollowup::Rejoin {
            schedule: ClassicRejoinSchedule::new(
                attempt.cycle(),
                Some(attempt.assignment_generation()),
                due,
            ),
            coordinator: super::ClassicCoordinatorRecovery::Rediscover,
        }
    }
}

enum RejectionFollowup {
    Rejoin {
        schedule: ClassicRejoinSchedule,
        coordinator: super::ClassicCoordinatorRecovery,
    },
    Fatal(ClassicGroupFatal),
}
