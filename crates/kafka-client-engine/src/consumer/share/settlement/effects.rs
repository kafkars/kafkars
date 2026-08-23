//! Exact consumption of assignment, revocation, and fatal core effects.

use kafka_client_core::ShareGroupHeartbeatEffect;

use super::super::membership::{ShareMembershipError, ShareMembershipInterpreter};

impl ShareMembershipInterpreter {
    pub(in crate::consumer::share) fn consume_success_effect(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
    ) -> Result<(), ShareMembershipError> {
        let mut effects = transition.into_effects();
        match effects.next() {
            Some(ShareGroupHeartbeatEffect::ReplaceAssignment {
                previous,
                assignment,
                member_epoch,
                schedule,
            }) if previous.as_ref() == self.activated_assignment.as_ref()
                && self.machine.live_assignment() == Some(&assignment)
                && self.machine.member_epoch() == Some(member_epoch)
                && self.machine.schedule() == Some(schedule) =>
            {
                drop(self.activated_assignment.take());
                drop(previous);
                self.activated_assignment = Some(assignment);
            }
            Some(ShareGroupHeartbeatEffect::AwaitAssignment {
                member_epoch,
                schedule,
            }) if self.activated_assignment.is_none()
                && self.machine.live_assignment().is_none()
                && self.machine.member_epoch() == Some(member_epoch)
                && self.machine.schedule() == Some(schedule) => {}
            Some(ShareGroupHeartbeatEffect::ArmHeartbeat { schedule })
                if self.activated_assignment.as_ref() == self.machine.live_assignment()
                    && self.machine.schedule() == Some(schedule) => {}
            _ => return Err(ShareMembershipError::EffectShape),
        }
        if effects.next().is_some() {
            return Err(ShareMembershipError::EffectShape);
        }
        Ok(())
    }

    pub(in crate::consumer::share) fn consume_terminal(
        &mut self,
        transition: kafka_client_core::ShareGroupHeartbeatTransition,
    ) -> Result<(), ShareMembershipError> {
        let mut effects = transition.into_effects().peekable();
        if matches!(
            effects.peek(),
            Some(ShareGroupHeartbeatEffect::Revoke { .. })
        ) {
            let Some(ShareGroupHeartbeatEffect::Revoke { assignment }) = effects.next() else {
                unreachable!("peeked exact revoke effect")
            };
            if self.activated_assignment.as_ref() != Some(&assignment) {
                return Err(ShareMembershipError::EffectShape);
            }
            drop(self.activated_assignment.take());
            drop(assignment);
        }
        let Some(ShareGroupHeartbeatEffect::Fatal { fatal }) = effects.next() else {
            return Err(ShareMembershipError::EffectShape);
        };
        if effects.next().is_some()
            || self.machine.fatal() != Some(fatal)
            || self.machine.phase() != kafka_client_core::ShareGroupHeartbeatPhase::Fatal
        {
            return Err(ShareMembershipError::EffectShape);
        }
        Ok(())
    }
}
