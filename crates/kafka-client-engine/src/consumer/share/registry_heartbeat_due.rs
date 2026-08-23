//! One deterministic share heartbeat deadline or cadence transition per turn.

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    entry::ShareConsumerEntry, registry::ShareConsumerRegistry,
    registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareHeartbeatDueTurn {
    Idle,
    Progress,
}

impl ShareConsumerRegistry {
    pub(super) fn prepare_one_heartbeat_due(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ShareHeartbeatDueTurn, ShareMembershipHostError> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| heartbeat_is_due(entry, now))
        else {
            return Ok(ShareHeartbeatDueTurn::Idle);
        };
        let membership = entry
            .membership
            .as_mut()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        if membership.prepared().is_some() && membership.expire_prepared_deadline(now)? {
            return Ok(ShareHeartbeatDueTurn::Progress);
        }
        if let Some(schedule) = membership.machine().retry_schedule() {
            if schedule.not_before().is_elapsed_at(now) {
                let _turn = membership.observe_retry_due(schedule, now)?;
                return Ok(ShareHeartbeatDueTurn::Progress);
            }
            return Ok(ShareHeartbeatDueTurn::Idle);
        }
        if membership.prepare_heartbeat_due(now, clock)? {
            return Ok(ShareHeartbeatDueTurn::Progress);
        }
        Ok(ShareHeartbeatDueTurn::Idle)
    }
}

fn heartbeat_is_due(entry: &ShareConsumerEntry, now: Moment) -> bool {
    if entry.has_close() || entry.fault.is_some() || entry.heartbeat_call.is_some() {
        return false;
    }
    let Some(membership) = entry.membership.as_ref() else {
        return false;
    };
    if let Some(retry) = membership.machine().retry_schedule() {
        return retry.not_before().is_elapsed_at(now);
    }
    if let Some(prepared) = membership.prepared() {
        return prepared.deadline.core().is_elapsed_at(now);
    }
    membership
        .machine()
        .schedule()
        .is_some_and(|schedule| schedule.deadline().is_elapsed_at(now))
}
