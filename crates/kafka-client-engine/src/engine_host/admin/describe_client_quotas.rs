//! Fair host turns for `AnyBroker` Admin `DescribeClientQuotas` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeClientQuotasShardLockError, DescribeClientQuotasShardWake,
        DescribeClientQuotasShardWakeError, DescribeClientQuotasTurn,
    },
    driver::{DescribeClientQuotasCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeClientQuotasProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeClientQuotasProgress, EngineHostError> {
    let mut host = match resources.describe_client_quotas.try_host() {
        Ok(host) => host,
        Err(DescribeClientQuotasShardLockError::Contended) => {
            return Ok(DescribeClientQuotasProgress::contended());
        }
        Err(DescribeClientQuotasShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeClientQuotasLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_client_quotas.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeClientQuotas)?;
    let driver_progress = match turn {
        DescribeClientQuotasTurn::Idle => false,
        DescribeClientQuotasTurn::Progress => true,
        DescribeClientQuotasTurn::Submit(submission) => {
            let (operation_id, deadline, plan, request_scratch_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeClientQuotasCall::submit(
                driver,
                plan,
                request_scratch_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeClientQuotas)?,
                Err(rejection) => {
                    let (plan, request_scratch_limit, result_limit) = rejection.into_correlation();
                    host.reject_handoff(operation_id, plan, request_scratch_limit, result_limit)
                        .map_err(EngineHostError::DescribeClientQuotas)?;
                }
            }
            true
        }
    };
    Ok(DescribeClientQuotasProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeClientQuotasProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeClientQuotasShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeClientQuotasShardWakeError> {
        self.request()
            .map_err(|error| DescribeClientQuotasShardWakeError::from_io(error.into_io()))
    }
}
