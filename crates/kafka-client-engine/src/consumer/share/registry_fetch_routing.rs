//! Fair hosted assignment routing into retained broker-local session plans.

use std::time::Duration;

use kafka_client_core::{AssignmentGeneration, Moment};

use crate::{clock::MonotonicClock, driver::DriverOwner};

use super::{
    entry::ShareConsumerEntry,
    fetch_routing::{ShareFetchRoutingOwner, ShareFetchRoutingTurn},
    fetch_state::ShareFetchRoutingFault,
    registry::ShareConsumerRegistry,
    registry_membership::ShareMembershipHostError,
};

const SHARE_FETCH_ROUTE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchRoutingHostTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn turn_one_fetch_routing(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<ShareFetchRoutingHostTurn, ShareMembershipHostError> {
        let mut blocked = false;
        for entry in &mut self.entries {
            if !fetch_routing_has_work(entry) {
                continue;
            }
            match turn_fetch_routing_entry(entry, now, clock, driver)? {
                ShareFetchRoutingHostTurn::Progress => {
                    return Ok(ShareFetchRoutingHostTurn::Progress);
                }
                ShareFetchRoutingHostTurn::Blocked => blocked = true,
                ShareFetchRoutingHostTurn::Idle => {}
            }
        }
        Ok(if blocked {
            ShareFetchRoutingHostTurn::Blocked
        } else {
            ShareFetchRoutingHostTurn::Idle
        })
    }
}

fn turn_fetch_routing_entry(
    entry: &mut ShareConsumerEntry,
    now: Moment,
    clock: &MonotonicClock,
    driver: &DriverOwner,
) -> Result<ShareFetchRoutingHostTurn, ShareMembershipHostError> {
    let target_generation = current_generation(entry);
    let closing = entry.has_close();
    let membership_faulted = entry.fault.is_some();
    let (fetch, membership) = entry.fetch_and_membership();
    if let Some(routing) = fetch.routing_mut() {
        let abandon = closing || target_generation != Some(routing.generation());
        if abandon && !routing.has_active_call() {
            drop(fetch.take_routing());
            return Ok(ShareFetchRoutingHostTurn::Progress);
        }
        return match routing.turn(driver, now) {
            ShareFetchRoutingTurn::Progress => Ok(ShareFetchRoutingHostTurn::Progress),
            ShareFetchRoutingTurn::Blocked => Ok(ShareFetchRoutingHostTurn::Blocked),
            ShareFetchRoutingTurn::Complete if abandon => {
                drop(fetch.take_routing());
                Ok(ShareFetchRoutingHostTurn::Progress)
            }
            ShareFetchRoutingTurn::Complete => {
                let membership = membership.ok_or(ShareMembershipHostError::EffectShape)?;
                let routed = routing
                    .try_take_routed_assignment(&membership.catalog)
                    .map_err(|_error| ShareMembershipHostError::EffectShape)?;
                if let Some(_routed) = fetch.install_routed(routed) {
                    return Err(ShareMembershipHostError::EffectShape);
                }
                drop(fetch.take_routing());
                Ok(ShareFetchRoutingHostTurn::Progress)
            }
            ShareFetchRoutingTurn::Faulted(kind) => {
                let generation = routing.generation();
                if !fetch.retain_fault(ShareFetchRoutingFault::new(generation, kind)) {
                    return Err(ShareMembershipHostError::EffectShape);
                }
                drop(fetch.take_routing());
                Ok(ShareFetchRoutingHostTurn::Progress)
            }
        };
    }
    if let Some(routed) = fetch.routed() {
        if closing || target_generation != Some(routed.generation()) {
            drop(fetch.take_routed());
            return Ok(ShareFetchRoutingHostTurn::Progress);
        }
        return Ok(ShareFetchRoutingHostTurn::Idle);
    }
    if let Some(fault) = fetch.fault() {
        if closing || target_generation != Some(fault.generation()) {
            fetch.clear_fault();
            return Ok(ShareFetchRoutingHostTurn::Progress);
        }
        return Ok(ShareFetchRoutingHostTurn::Idle);
    }
    if closing || membership_faulted {
        return Ok(ShareFetchRoutingHostTurn::Idle);
    }
    let membership = membership.ok_or(ShareMembershipHostError::EffectShape)?;
    let Some(assignment) = membership.activated_assignment() else {
        return Ok(ShareFetchRoutingHostTurn::Idle);
    };
    let capture = clock
        .capture_deadline_after(SHARE_FETCH_ROUTE_TIMEOUT)
        .map_err(|_error| {
            ShareMembershipHostError::Membership(super::ShareMembershipError::DeadlineMapping)
        })?;
    let routing = ShareFetchRoutingOwner::try_begin(&membership.catalog, assignment, capture)
        .map_err(|_error| ShareMembershipHostError::EffectShape)?;
    if let Some(_routing) = fetch.install_routing(routing) {
        return Err(ShareMembershipHostError::EffectShape);
    }
    Ok(ShareFetchRoutingHostTurn::Progress)
}

pub(super) fn current_generation(entry: &ShareConsumerEntry) -> Option<AssignmentGeneration> {
    entry
        .membership
        .as_ref()
        .and_then(super::ShareMembershipInterpreter::activated_assignment)
        .map(kafka_client_core::LiveGroupAssignment::assignment_generation)
}

fn fetch_routing_has_work(entry: &ShareConsumerEntry) -> bool {
    if entry.fetch().sessions().is_some() || entry.fetch().session_fault().is_some() {
        return false;
    }
    if entry.fetch().routing().is_some() {
        return true;
    }
    let target = current_generation(entry);
    if let Some(routed) = entry.fetch().routed() {
        return entry.has_close() || target != Some(routed.generation());
    }
    if let Some(fault) = entry.fetch().fault() {
        return entry.has_close() || target != Some(fault.generation());
    }
    !entry.has_close()
        && entry.fault.is_none()
        && entry
            .membership
            .as_ref()
            .and_then(super::ShareMembershipInterpreter::activated_assignment)
            .is_some()
}
