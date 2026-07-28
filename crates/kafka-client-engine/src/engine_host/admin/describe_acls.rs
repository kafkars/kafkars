//! Fair host turns for AnyBroker Admin `DescribeAcls` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeAclsShardLockError, DescribeAclsShardWake, DescribeAclsShardWakeError,
        DescribeAclsTurn,
    },
    driver::{DescribeAclsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeAclsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeAclsProgress, EngineHostError> {
    let mut host = match resources.describe_acls.try_host() {
        Ok(host) => host,
        Err(DescribeAclsShardLockError::Contended) => {
            return Ok(DescribeAclsProgress::contended());
        }
        Err(DescribeAclsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeAclsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_acls.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeAcls)?;
    let driver_progress = match turn {
        DescribeAclsTurn::Idle => false,
        DescribeAclsTurn::Progress => true,
        DescribeAclsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_request_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeAclsCall::submit(
                driver,
                plan,
                retained_request_bytes,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeAcls)?,
                Err(rejection) => {
                    let (plan, result_limit) = rejection.into_correlation();
                    host.reject_handoff(operation_id, plan, result_limit)
                        .map_err(EngineHostError::DescribeAcls)?;
                }
            }
            true
        }
    };
    Ok(DescribeAclsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeAclsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeAclsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeAclsShardWakeError> {
        self.request()
            .map_err(|error| DescribeAclsShardWakeError::from_io(error.into_io()))
    }
}
