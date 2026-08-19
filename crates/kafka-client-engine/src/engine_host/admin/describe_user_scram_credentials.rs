//! Fair host turns for `AnyBroker` Admin `DescribeUserScramCredentials` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeUserScramCredentialsShardLockError, DescribeUserScramCredentialsShardWake,
        DescribeUserScramCredentialsShardWakeError, DescribeUserScramCredentialsTurn,
    },
    driver::{DescribeUserScramCredentialsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeUserScramCredentialsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeUserScramCredentialsProgress, EngineHostError> {
    let mut host = match resources.describe_user_scram_credentials.try_host() {
        Ok(host) => host,
        Err(DescribeUserScramCredentialsShardLockError::Contended) => {
            return Ok(DescribeUserScramCredentialsProgress::contended());
        }
        Err(DescribeUserScramCredentialsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeUserScramCredentialsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .describe_user_scram_credentials
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeUserScramCredentials)?;
    let driver_progress = match turn {
        DescribeUserScramCredentialsTurn::Idle => false,
        DescribeUserScramCredentialsTurn::Progress => true,
        DescribeUserScramCredentialsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, request_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeUserScramCredentialsCall::submit(
                driver,
                plan,
                request_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeUserScramCredentials)?,
                Err(rejection) => {
                    let (plan, request_limit, result_limit) = rejection.into_evidence();
                    host.reject_handoff(operation_id, plan, request_limit, result_limit)
                        .map_err(EngineHostError::DescribeUserScramCredentials)?;
                }
            }
            true
        }
    };
    Ok(DescribeUserScramCredentialsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeUserScramCredentialsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeUserScramCredentialsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeUserScramCredentialsShardWakeError> {
        self.request()
            .map_err(|error| DescribeUserScramCredentialsShardWakeError::from_io(error.into_io()))
    }
}
