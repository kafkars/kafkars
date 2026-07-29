//! Fair host turns for one secret-bearing Admin `CreateDelegationToken` operation.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        CreateDelegationTokenShardLockError, CreateDelegationTokenShardWake,
        CreateDelegationTokenShardWakeError, CreateDelegationTokenTurn,
    },
    driver::{CreateDelegationTokenCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct CreateDelegationTokenProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<CreateDelegationTokenProgress, EngineHostError> {
    let mut host = match resources.create_delegation_token.try_host() {
        Ok(host) => host,
        Err(CreateDelegationTokenShardLockError::Contended) => {
            return Ok(CreateDelegationTokenProgress::contended());
        }
        Err(CreateDelegationTokenShardLockError::Poisoned) => {
            return Err(EngineHostError::CreateDelegationTokenLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.create_delegation_token.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::CreateDelegationToken)?;
    let driver_progress = match turn {
        CreateDelegationTokenTurn::Idle => false,
        CreateDelegationTokenTurn::Progress => true,
        CreateDelegationTokenTurn::Submit(submission) => {
            let (operation_id, deadline, _plan, request) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match CreateDelegationTokenCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::CreateDelegationToken)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::CreateDelegationToken)?,
            }
            true
        }
    };
    Ok(CreateDelegationTokenProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl CreateDelegationTokenProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl CreateDelegationTokenShardWake for ReactorWake {
    fn wake(&self) -> Result<(), CreateDelegationTokenShardWakeError> {
        self.request()
            .map_err(|error| CreateDelegationTokenShardWakeError::from_io(error.into_io()))
    }
}
