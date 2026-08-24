//! Failure classification into retry, recovery, leave, or terminal ownership.

use kafka_client_core::{
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatInput, ShareGroupHeartbeatRequestKind,
    ShareGroupHeartbeatRetryCause,
};

use super::super::{
    membership::{
        ShareMembershipError, ShareMembershipFailureTurn, ShareMembershipInterpreter,
        ShareMembershipRetryGate,
    },
    prepared::PreparedShareGroupHeartbeat,
    transition::{consume_close_effects, map_core},
};

impl ShareMembershipInterpreter {
    pub(in crate::consumer::share) fn settle_failure(
        &mut self,
        now: kafka_client_core::Moment,
        clock: &crate::clock::MonotonicClock,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<ShareMembershipFailureTurn, ShareMembershipError> {
        let prepared = self.prepared.ok_or(ShareMembershipError::EffectShape)?;
        if failure == ShareGroupHeartbeatFailure::Broker(14) {
            let transition = self
                .machine
                .apply(ShareGroupHeartbeatInput::RetryCoordinatorLoad {
                    attempt: prepared.attempt,
                    now,
                    failure,
                })
                .map_err(map_core)?;
            if transition_is_terminal(&transition) {
                self.finish_terminal_transition(transition)?;
                return Ok(ShareMembershipFailureTurn::Terminal);
            }
            return self.install_retry(transition, ShareGroupHeartbeatRetryCause::CoordinatorLoad);
        }
        if matches!(
            failure,
            ShareGroupHeartbeatFailure::CoordinatorUnavailable
                | ShareGroupHeartbeatFailure::Broker(15 | 16)
        ) {
            let transition = self
                .machine
                .apply(ShareGroupHeartbeatInput::RetryHeartbeat {
                    attempt: prepared.attempt,
                    now,
                    failure,
                })
                .map_err(map_core)?;
            if transition_is_terminal(&transition) {
                self.finish_terminal_transition(transition)?;
                return Ok(ShareMembershipFailureTurn::Terminal);
            }
            return self.install_rediscovery(transition, clock, prepared);
        }
        if prepared.kind == ShareGroupHeartbeatRequestKind::Steady
            && matches!(failure, ShareGroupHeartbeatFailure::Broker(25 | 110))
        {
            let transition = self
                .machine
                .apply(ShareGroupHeartbeatInput::RecoverFencedMembership {
                    attempt: prepared.attempt,
                    now,
                    failure,
                })
                .map_err(map_core)?;
            if transition_is_terminal(&transition) {
                self.finish_terminal_transition(transition)?;
                return Ok(ShareMembershipFailureTurn::Terminal);
            }
            self.install_rejoin(transition, clock, prepared)?;
            return Ok(ShareMembershipFailureTurn::Rejoin);
        }
        self.apply_terminal(prepared, failure)?;
        Ok(ShareMembershipFailureTurn::Terminal)
    }

    fn finish_terminal_transition(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
    ) -> Result<(), ShareMembershipError> {
        self.consume_terminal(transition)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        Ok(())
    }

    pub(in crate::consumer::share) fn fail_rediscovery(
        &mut self,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<(), ShareMembershipError> {
        let schedule = self
            .machine
            .retry_schedule()
            .ok_or(ShareMembershipError::EffectShape)?;
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::RediscoveryFailed { schedule, failure })
            .map_err(map_core)?;
        self.consume_terminal(transition)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        Ok(())
    }

    pub(in crate::consumer::share) fn settle_leave_success(
        &mut self,
    ) -> Result<(), ShareMembershipError> {
        let prepared = self.prepared.ok_or(ShareMembershipError::EffectShape)?;
        if prepared.kind != ShareGroupHeartbeatRequestKind::Leave {
            return Err(ShareMembershipError::EffectShape);
        }
        let transition = self
            .machine
            .apply(ShareGroupHeartbeatInput::LeaveSucceeded {
                attempt: prepared.attempt,
            })
            .map_err(map_core)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        consume_close_effects(self, transition)
    }

    pub(in crate::consumer::share) fn apply_terminal(
        &mut self,
        prepared: PreparedShareGroupHeartbeat,
        failure: ShareGroupHeartbeatFailure,
    ) -> Result<(), ShareMembershipError> {
        let input = match prepared.kind {
            ShareGroupHeartbeatRequestKind::Join | ShareGroupHeartbeatRequestKind::Steady => {
                ShareGroupHeartbeatInput::HeartbeatFailed {
                    attempt: prepared.attempt,
                    failure,
                }
            }
            ShareGroupHeartbeatRequestKind::Leave => ShareGroupHeartbeatInput::LeaveFailed {
                attempt: prepared.attempt,
                failure,
            },
        };
        let transition = self.machine.apply(input).map_err(map_core)?;
        self.consume_terminal(transition)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        Ok(())
    }
}

fn transition_is_terminal(transition: &kafka_client_core::ShareGroupHeartbeatTransition) -> bool {
    transition.effects().any(|effect| {
        matches!(
            effect,
            kafka_client_core::ShareGroupHeartbeatEffect::Fatal { .. }
        )
    })
}
