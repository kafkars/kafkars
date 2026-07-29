//! Fair host turns for one secret-bearing Admin `RenewDelegationToken` operation.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        RenewDelegationTokenShardLockError, RenewDelegationTokenShardWake,
        RenewDelegationTokenShardWakeError, RenewDelegationTokenTurn,
    },
    driver::{ReactorWake, RenewDelegationTokenCall},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct RenewDelegationTokenProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<RenewDelegationTokenProgress, EngineHostError> {
    let mut host = match resources.renew_delegation_token.try_host() {
        Ok(host) => host,
        Err(RenewDelegationTokenShardLockError::Contended) => {
            return Ok(RenewDelegationTokenProgress::contended());
        }
        Err(RenewDelegationTokenShardLockError::Poisoned) => {
            return Err(EngineHostError::RenewDelegationTokenLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.renew_delegation_token.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::RenewDelegationToken)?;
    let driver_progress = match turn {
        RenewDelegationTokenTurn::Idle => false,
        RenewDelegationTokenTurn::Progress => true,
        RenewDelegationTokenTurn::Submit(submission) => {
            let (operation_id, deadline, _plan, request) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match RenewDelegationTokenCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::RenewDelegationToken)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::RenewDelegationToken)?,
            }
            true
        }
    };
    Ok(RenewDelegationTokenProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl RenewDelegationTokenProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl RenewDelegationTokenShardWake for ReactorWake {
    fn wake(&self) -> Result<(), RenewDelegationTokenShardWakeError> {
        self.request()
            .map_err(|error| RenewDelegationTokenShardWakeError::from_io(error.into_io()))
    }
}
