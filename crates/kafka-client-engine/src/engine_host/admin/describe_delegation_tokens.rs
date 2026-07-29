//! Fair host turns for one secret-bearing Admin `DescribeDelegationTokens` operation.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeDelegationTokensShardLockError, DescribeDelegationTokensShardWake,
        DescribeDelegationTokensShardWakeError, DescribeDelegationTokensTurn,
    },
    driver::{DescribeDelegationTokensCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeDelegationTokensProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeDelegationTokensProgress, EngineHostError> {
    let mut host = match resources.describe_delegation_tokens.try_host() {
        Ok(host) => host,
        Err(DescribeDelegationTokensShardLockError::Contended) => {
            return Ok(DescribeDelegationTokensProgress::contended());
        }
        Err(DescribeDelegationTokensShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeDelegationTokensLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_delegation_tokens.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeDelegationTokens)?;
    let driver_progress = match turn {
        DescribeDelegationTokensTurn::Idle => false,
        DescribeDelegationTokensTurn::Progress => true,
        DescribeDelegationTokensTurn::Submit(submission) => {
            let (operation_id, deadline, _plan, request) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeDelegationTokensCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeDelegationTokens)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DescribeDelegationTokens)?,
            }
            true
        }
    };
    Ok(DescribeDelegationTokensProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeDelegationTokensProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeDelegationTokensShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeDelegationTokensShardWakeError> {
        self.request()
            .map_err(|error| DescribeDelegationTokensShardWakeError::from_io(error.into_io()))
    }
}
