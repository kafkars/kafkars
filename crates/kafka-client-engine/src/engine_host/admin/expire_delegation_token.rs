//! Fair host turns for one secret-bearing Admin `ExpireDelegationToken` operation.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        ExpireDelegationTokenShardLockError, ExpireDelegationTokenShardWake,
        ExpireDelegationTokenShardWakeError, ExpireDelegationTokenTurn,
    },
    driver::{ExpireDelegationTokenCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ExpireDelegationTokenProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ExpireDelegationTokenProgress, EngineHostError> {
    let mut host = match resources.expire_delegation_token.try_host() {
        Ok(host) => host,
        Err(ExpireDelegationTokenShardLockError::Contended) => {
            return Ok(ExpireDelegationTokenProgress::contended());
        }
        Err(ExpireDelegationTokenShardLockError::Poisoned) => {
            return Err(EngineHostError::ExpireDelegationTokenLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.expire_delegation_token.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ExpireDelegationToken)?;
    let driver_progress = match turn {
        ExpireDelegationTokenTurn::Idle => false,
        ExpireDelegationTokenTurn::Progress => true,
        ExpireDelegationTokenTurn::Submit(submission) => {
            let (operation_id, deadline, _plan, request) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match ExpireDelegationTokenCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ExpireDelegationToken)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::ExpireDelegationToken)?,
            }
            true
        }
    };
    Ok(ExpireDelegationTokenProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ExpireDelegationTokenProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl ExpireDelegationTokenShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ExpireDelegationTokenShardWakeError> {
        self.request()
            .map_err(|error| ExpireDelegationTokenShardWakeError::from_io(error.into_io()))
    }
}
