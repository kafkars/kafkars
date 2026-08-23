//! Successful heartbeat validation and assignment-effect installation.

use kafka_client_core::{
    ShareGroupHeartbeatFailure, ShareGroupHeartbeatInput, ShareGroupMemberEpoch,
};

use crate::protocol::consumer::share_group::ShareGroupHeartbeatSuccess;

use super::super::membership::{
    ShareMembershipError, ShareMembershipInterpreter, ShareMembershipRetryGate,
};

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

impl ShareMembershipInterpreter {
    pub(in crate::consumer::share) fn settle_success(
        &mut self,
        now: kafka_client_core::Moment,
        success: ShareGroupHeartbeatSuccess,
    ) -> Result<(), ShareMembershipError> {
        let prepared = self.prepared.ok_or(ShareMembershipError::EffectShape)?;
        let (throttle_time_ms, member, member_epoch, heartbeat_interval_ms, assignment) =
            success.into_parts();
        if member
            .as_ref()
            .is_some_and(|member| member.as_ref() != self.catalog.member())
        {
            return self.apply_terminal(prepared, ShareGroupHeartbeatFailure::InvalidResponse);
        }
        let Some(member_epoch) = ShareGroupMemberEpoch::try_from_raw(member_epoch) else {
            return self.apply_terminal(prepared, ShareGroupHeartbeatFailure::InvalidResponse);
        };
        let heartbeat_interval_ticks = u64::from(heartbeat_interval_ms)
            .checked_mul(TICKS_PER_MILLISECOND)
            .ok_or(ShareMembershipError::EffectShape)?;
        let throttle_ticks = u64::from(throttle_time_ms)
            .checked_mul(TICKS_PER_MILLISECOND)
            .ok_or(ShareMembershipError::EffectShape)?;
        let assignment = match assignment
            .as_deref()
            .map(|assignment| self.catalog.translate_assignment(assignment))
            .transpose()
        {
            Ok(assignment) => assignment,
            Err(_error) => {
                return self.apply_terminal(prepared, ShareGroupHeartbeatFailure::InvalidResponse);
            }
        };
        let transition = match self
            .machine
            .apply(ShareGroupHeartbeatInput::HeartbeatSucceeded {
                attempt: prepared.attempt,
                now,
                member_epoch,
                heartbeat_interval_ticks,
                throttle_ticks,
                assignment,
            }) {
            Ok(transition) => transition,
            Err(error)
                if error.kind()
                    == kafka_client_core::ShareGroupHeartbeatErrorKind::DeadlineElapsed =>
            {
                return self.apply_terminal(prepared, ShareGroupHeartbeatFailure::DeadlineElapsed);
            }
            Err(_error) => {
                return self.apply_terminal(prepared, ShareGroupHeartbeatFailure::InvalidResponse);
            }
        };
        self.consume_success_effect(transition)?;
        self.prepared = None;
        self.retry_gate = ShareMembershipRetryGate::Open;
        Ok(())
    }
}
