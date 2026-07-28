//! Fair host turns for exact-broker Admin `DescribeLogDirs` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeLogDirsShardLockError, DescribeLogDirsShardWake, DescribeLogDirsShardWakeError,
        DescribeLogDirsTurn,
    },
    driver::{DescribeLogDirsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeLogDirsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeLogDirsProgress, EngineHostError> {
    let mut host = match resources.describe_log_dirs.try_host() {
        Ok(host) => host,
        Err(DescribeLogDirsShardLockError::Contended) => {
            return Ok(DescribeLogDirsProgress::contended());
        }
        Err(DescribeLogDirsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeLogDirsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_log_dirs.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeLogDirs)?;
    let driver_progress = match turn {
        DescribeLogDirsTurn::Idle => false,
        DescribeLogDirsTurn::Progress => true,
        DescribeLogDirsTurn::Submit(submission) => {
            let (operation_id, deadline, broker_id) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeLogDirsCall::submit(driver, broker_id, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeLogDirs)?,
                Err(rejection) => {
                    drop(rejection);
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::DescribeLogDirs)?;
                }
            }
            true
        }
    };
    Ok(DescribeLogDirsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeLogDirsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeLogDirsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeLogDirsShardWakeError> {
        self.request()
            .map_err(|error| DescribeLogDirsShardWakeError::from_io(error.into_io()))
    }
}
