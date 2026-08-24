//! Fair hosted opening, execution, and abandonment of broker-local share sessions.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, Moment};

use crate::{clock::MonotonicClock, driver::DriverOwner};

use super::{
    entry::ShareConsumerEntry,
    fetch_session_set::{
        ShareFetchSessionIdentity, ShareFetchSessionSet, ShareFetchSessionSetTurn,
    },
    fetch_state::{ShareFetchSessionFault, ShareFetchSessionFaultKind},
    registry::ShareConsumerRegistry,
    registry_fetch_routing::current_generation,
    registry_membership::ShareMembershipHostError,
};

mod abandon;
use abandon::{abandon_sessions, fetch_sessions_have_work};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionsHostTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn turn_one_fetch_sessions(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
        let mut blocked = false;
        for entry in &mut self.entries {
            if !fetch_sessions_have_work(entry) {
                continue;
            }
            match turn_fetch_sessions_entry(entry, now, clock, driver)? {
                ShareFetchSessionsHostTurn::Progress => {
                    return Ok(ShareFetchSessionsHostTurn::Progress);
                }
                ShareFetchSessionsHostTurn::Blocked => blocked = true,
                ShareFetchSessionsHostTurn::Idle => {}
            }
        }
        Ok(if blocked {
            ShareFetchSessionsHostTurn::Blocked
        } else {
            ShareFetchSessionsHostTurn::Idle
        })
    }
}

fn turn_fetch_sessions_entry(
    entry: &mut ShareConsumerEntry,
    now: Moment,
    clock: &MonotonicClock,
    driver: &DriverOwner,
) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
    let generation = current_generation(entry);
    let abandon = entry.has_close()
        || entry
            .fetch()
            .sessions()
            .is_some_and(|sessions| generation != Some(sessions.generation()));
    if entry.fetch().sessions().is_some() {
        let recovering = entry
            .fetch()
            .sessions()
            .is_some_and(super::fetch_session_set::ShareFetchSessionSet::is_recovering);
        if abandon && !recovering {
            return abandon_sessions(entry, true);
        }
        if entry.fetch().session_fault().is_some() && !recovering {
            return abandon_sessions(entry, false);
        }
        let turn = entry
            .fetch_mut()
            .sessions_mut()
            .ok_or(ShareMembershipHostError::EffectShape)?
            .turn(driver, now);
        return match turn {
            Ok(ShareFetchSessionSetTurn::Progress) => Ok(ShareFetchSessionsHostTurn::Progress),
            Ok(ShareFetchSessionSetTurn::Blocked) => Ok(ShareFetchSessionsHostTurn::Blocked),
            Ok(ShareFetchSessionSetTurn::Idle) => Ok(ShareFetchSessionsHostTurn::Idle),
            Ok(ShareFetchSessionSetTurn::NeedsPreparation(index)) => {
                prepare_next_session(entry, generation, index, clock)
            }
            Ok(ShareFetchSessionSetTurn::Released) => Err(ShareMembershipHostError::EffectShape),
            Ok(ShareFetchSessionSetTurn::RecoveryReady) => abandon_sessions(entry, false),
            Err(error) => {
                let generation = generation.ok_or(ShareMembershipHostError::EffectShape)?;
                if !entry
                    .fetch_mut()
                    .retain_session_fault(ShareFetchSessionFault::new(
                        generation,
                        ShareFetchSessionFaultKind::Execution(error),
                    ))
                {
                    return Err(ShareMembershipHostError::EffectShape);
                }
                Ok(ShareFetchSessionsHostTurn::Progress)
            }
        };
    }
    if let Some(fault) = entry.fetch().session_fault() {
        if entry.has_close() || generation != Some(fault.generation()) {
            entry.fetch_mut().clear_session_fault();
            return Ok(ShareFetchSessionsHostTurn::Progress);
        }
        return Ok(ShareFetchSessionsHostTurn::Idle);
    }
    let Some(routed) = entry.fetch().routed() else {
        return Ok(ShareFetchSessionsHostTurn::Idle);
    };
    if entry.has_close() || generation != Some(routed.generation()) {
        drop(entry.fetch_mut().take_routed());
        return Ok(ShareFetchSessionsHostTurn::Progress);
    }
    open_sessions(entry, clock)
}

fn open_sessions(
    entry: &mut ShareConsumerEntry,
    clock: &MonotonicClock,
) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
    let membership = entry
        .membership
        .as_ref()
        .ok_or(ShareMembershipHostError::EffectShape)?;
    let member_epoch = membership
        .machine()
        .member_epoch()
        .ok_or(ShareMembershipHostError::EffectShape)?;
    let generation = membership
        .activated_assignment()
        .map(kafka_client_core::LiveGroupAssignment::assignment_generation)
        .ok_or(ShareMembershipHostError::EffectShape)?;
    let capture = match clock.capture_deadline_after(entry.fetch().config().attempt_timeout()) {
        Ok(capture) => capture,
        Err(_error) => {
            drop(entry.fetch_mut().take_routed());
            return retain_open_fault(
                entry,
                generation,
                ShareFetchSessionFaultKind::DeadlineMapping,
            );
        }
    };
    let group_id = entry.group_id();
    let member_id = entry.member_id();
    let group = Arc::clone(entry.group());
    let member = Arc::clone(entry.member());
    let config = entry.fetch().config();
    let routed = entry
        .fetch_mut()
        .take_routed()
        .ok_or(ShareMembershipHostError::EffectShape)?;
    let identity = ShareFetchSessionIdentity::new(group_id, member_id, member_epoch, group, member);
    match ShareFetchSessionSet::try_open(routed, &identity, config, capture) {
        Ok(sessions) => {
            if let Some(sessions) = entry.fetch_mut().install_sessions(sessions) {
                sessions
                    .release_unsubmitted()
                    .map_err(|_error| ShareMembershipHostError::EffectShape)?;
                return Err(ShareMembershipHostError::EffectShape);
            }
            Ok(ShareFetchSessionsHostTurn::Progress)
        }
        Err(error) => retain_open_fault(entry, generation, ShareFetchSessionFaultKind::Open(error)),
    }
}

fn retain_open_fault(
    entry: &mut ShareConsumerEntry,
    generation: AssignmentGeneration,
    kind: ShareFetchSessionFaultKind,
) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
    if !entry
        .fetch_mut()
        .retain_session_fault(ShareFetchSessionFault::new(generation, kind))
    {
        return Err(ShareMembershipHostError::EffectShape);
    }
    Ok(ShareFetchSessionsHostTurn::Progress)
}

fn prepare_next_session(
    entry: &mut ShareConsumerEntry,
    generation: Option<AssignmentGeneration>,
    index: usize,
    clock: &MonotonicClock,
) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
    let generation = generation.ok_or(ShareMembershipHostError::EffectShape)?;
    let capture = match clock.capture_deadline_after(entry.fetch().config().attempt_timeout()) {
        Ok(capture) => capture,
        Err(_error) => {
            return retain_open_fault(
                entry,
                generation,
                ShareFetchSessionFaultKind::DeadlineMapping,
            );
        }
    };
    let result = entry
        .fetch_mut()
        .sessions_mut()
        .ok_or(ShareMembershipHostError::EffectShape)?
        .prepare_session(index, capture);
    match result {
        Ok(()) => Ok(ShareFetchSessionsHostTurn::Progress),
        Err(error) => retain_open_fault(
            entry,
            generation,
            ShareFetchSessionFaultKind::Execution(
                super::fetch_session_execution::ShareFetchExecutionError::Session(error),
            ),
        ),
    }
}
