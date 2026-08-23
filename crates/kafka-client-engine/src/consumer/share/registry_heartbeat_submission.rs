//! One prepared `ShareGroupHeartbeat` v1 handoff to the tracked driver lane.

use kafka_client_core::{Moment, ShareGroupHeartbeatFailure};

use crate::{
    clock::MonotonicClock,
    driver::{
        DriverOwner,
        share_group_heartbeat::{ShareGroupHeartbeatCall, ShareGroupHeartbeatSubmitErrorKind},
    },
};

use super::{
    membership::ShareMembershipFailureTurn, registry::ShareConsumerRegistry,
    registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareHeartbeatSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn submit_one_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<ShareHeartbeatSubmissionTurn, ShareMembershipHostError> {
        let Some(entry) = self.entries.iter_mut().find(|entry| {
            entry.fault.is_none()
                && entry.heartbeat_call.is_none()
                && entry
                    .membership
                    .as_ref()
                    .is_some_and(super::ShareMembershipInterpreter::is_ready_to_submit)
        }) else {
            return Ok(ShareHeartbeatSubmissionTurn::Idle);
        };
        let membership = entry
            .membership
            .as_mut()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        let prepared = membership
            .prepared()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        if prepared.deadline.core().is_elapsed_at(now) {
            membership.expire_prepared_deadline(now)?;
            return Ok(ShareHeartbeatSubmissionTurn::Progress);
        }
        let request = match membership.prepare_request() {
            Ok(request) => request,
            Err(_error) => {
                settle_local_failure(
                    entry,
                    now,
                    clock,
                    ShareGroupHeartbeatFailure::InvalidResponse,
                )?;
                return Ok(ShareHeartbeatSubmissionTurn::Progress);
            }
        };
        match ShareGroupHeartbeatCall::submit(driver, entry.group(), request, prepared.deadline) {
            Ok(call) => {
                entry
                    .install_heartbeat_call(call)
                    .map_err(|_call| ShareMembershipHostError::EffectShape)?;
                Ok(ShareHeartbeatSubmissionTurn::Progress)
            }
            Err(error) if error.kind() == ShareGroupHeartbeatSubmitErrorKind::Full => {
                Ok(ShareHeartbeatSubmissionTurn::Blocked)
            }
            Err(_error) => {
                settle_local_failure(entry, now, clock, ShareGroupHeartbeatFailure::Execution)?;
                Ok(ShareHeartbeatSubmissionTurn::Progress)
            }
        }
    }
}

pub(super) fn settle_local_failure(
    entry: &mut super::entry::ShareConsumerEntry,
    now: Moment,
    clock: &MonotonicClock,
    failure: ShareGroupHeartbeatFailure,
) -> Result<(), ShareMembershipHostError> {
    let turn = entry
        .membership
        .as_mut()
        .ok_or(ShareMembershipHostError::EffectShape)?
        .settle_failure(now, clock, failure)?;
    match turn {
        ShareMembershipFailureTurn::Terminal
        | ShareMembershipFailureTurn::RetryScheduled(_)
        | ShareMembershipFailureTurn::Rejoin => Ok(()),
        ShareMembershipFailureTurn::Rediscovery(_) => Err(ShareMembershipHostError::EffectShape),
    }
}
