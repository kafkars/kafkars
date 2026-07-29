//! Fair host turns for destructive AnyBroker Admin `AlterUserScramCredentials`.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AlterUserScramCredentialsShardLockError, AlterUserScramCredentialsShardWake,
        AlterUserScramCredentialsShardWakeError, AlterUserScramCredentialsTurn,
    },
    driver::{AlterUserScramCredentialsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterUserScramCredentialsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterUserScramCredentialsProgress, EngineHostError> {
    let mut host = match resources.alter_user_scram_credentials.try_host() {
        Ok(host) => host,
        Err(AlterUserScramCredentialsShardLockError::Contended) => {
            return Ok(AlterUserScramCredentialsProgress::contended());
        }
        Err(AlterUserScramCredentialsShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterUserScramCredentialsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .alter_user_scram_credentials
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AlterUserScramCredentials)?;
    let driver_progress = match turn {
        AlterUserScramCredentialsTurn::Idle => false,
        AlterUserScramCredentialsTurn::Progress => true,
        AlterUserScramCredentialsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, request) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AlterUserScramCredentialsCall::submit(driver, plan, request, deadline.transport())
            {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterUserScramCredentials)?,
                Err(rejection) => {
                    drop(rejection);
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AlterUserScramCredentials)?;
                }
            }
            true
        }
    };
    Ok(AlterUserScramCredentialsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterUserScramCredentialsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AlterUserScramCredentialsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterUserScramCredentialsShardWakeError> {
        self.request()
            .map_err(|error| AlterUserScramCredentialsShardWakeError::from_io(error.into_io()))
    }
}
