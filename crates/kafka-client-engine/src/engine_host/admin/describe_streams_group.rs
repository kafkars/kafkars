//! Bounded host turns for read-only streams-group description.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::describe_streams_group::{
        DescribeStreamsGroupShardLockError, DescribeStreamsGroupShardWake,
        DescribeStreamsGroupShardWakeError, DescribeStreamsGroupTurn,
    },
    driver::{DescribeStreamsGroupCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeStreamsGroupProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeStreamsGroupProgress, EngineHostError> {
    let mut host = match resources.describe_streams_group.try_host() {
        Ok(host) => host,
        Err(DescribeStreamsGroupShardLockError::Contended) => {
            return Ok(DescribeStreamsGroupProgress::contended());
        }
        Err(DescribeStreamsGroupShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeStreamsGroupLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_streams_group.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeStreamsGroup)?;
    let driver_progress = match turn {
        DescribeStreamsGroupTurn::Idle => false,
        DescribeStreamsGroupTurn::Progress => true,
        DescribeStreamsGroupTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeStreamsGroupCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeStreamsGroup)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DescribeStreamsGroup)?,
            }
            true
        }
    };
    Ok(DescribeStreamsGroupProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeStreamsGroupProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeStreamsGroupShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeStreamsGroupShardWakeError> {
        self.request()
            .map_err(|error| DescribeStreamsGroupShardWakeError::from_io(error.into_io()))
    }
}
