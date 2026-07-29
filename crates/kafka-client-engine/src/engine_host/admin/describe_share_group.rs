//! Bounded host turns for read-only share-group description.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::describe_share_group::{
        DescribeShareGroupShardLockError, DescribeShareGroupShardWake,
        DescribeShareGroupShardWakeError, DescribeShareGroupTurn,
    },
    driver::{DescribeShareGroupCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeShareGroupProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeShareGroupProgress, EngineHostError> {
    let mut host = match resources.describe_share_group.try_host() {
        Ok(host) => host,
        Err(DescribeShareGroupShardLockError::Contended) => {
            return Ok(DescribeShareGroupProgress::contended());
        }
        Err(DescribeShareGroupShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeShareGroupLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_share_group.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeShareGroup)?;
    let driver_progress = match turn {
        DescribeShareGroupTurn::Idle => false,
        DescribeShareGroupTurn::Progress => true,
        DescribeShareGroupTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeShareGroupCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeShareGroup)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DescribeShareGroup)?,
            }
            true
        }
    };
    Ok(DescribeShareGroupProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeShareGroupProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeShareGroupShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeShareGroupShardWakeError> {
        self.request()
            .map_err(|error| DescribeShareGroupShardWakeError::from_io(error.into_io()))
    }
}
