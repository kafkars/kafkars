//! Fair host turns for controller-routed partition-election alteration.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{ElectLeadersShardLockError, ElectLeadersTurn},
    driver::ElectLeadersCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ElectLeadersProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ElectLeadersProgress, EngineHostError> {
    let mut host = match resources.elect_leaders.try_host() {
        Ok(host) => host,
        Err(ElectLeadersShardLockError::Contended) => {
            return Ok(ElectLeadersProgress::contended());
        }
        Err(ElectLeadersShardLockError::Poisoned) => {
            return Err(EngineHostError::ElectLeadersLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.elect_leaders.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::ElectLeaders)?;
    let driver_progress = match turn {
        ElectLeadersTurn::Idle => false,
        ElectLeadersTurn::Progress => true,
        ElectLeadersTurn::Submit(submission) => {
            let (operation_id, deadline, plan, scratch_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match ElectLeadersCall::submit(driver, &plan, scratch_limit, deadline, now) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ElectLeaders)?,
                Err(rejection) => {
                    drop(rejection);
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::ElectLeaders)?;
                }
            }
            true
        }
    };
    Ok(ElectLeadersProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ElectLeadersProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
