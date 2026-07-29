//! Fair host turns for selected-replica Admin `DescribeReplicaLogDirs` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DescribeReplicaLogDirsShardLockError, DescribeReplicaLogDirsShardWake,
        DescribeReplicaLogDirsShardWakeError, DescribeReplicaLogDirsTurn,
    },
    driver::{DescribeReplicaLogDirsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeReplicaLogDirsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeReplicaLogDirsProgress, EngineHostError> {
    let mut host = match resources.describe_replica_log_dirs.try_host() {
        Ok(host) => host,
        Err(DescribeReplicaLogDirsShardLockError::Contended) => {
            return Ok(DescribeReplicaLogDirsProgress::contended());
        }
        Err(DescribeReplicaLogDirsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeReplicaLogDirsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_replica_log_dirs.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeReplicaLogDirs)?;
    let driver_progress = match turn {
        DescribeReplicaLogDirsTurn::Idle => false,
        DescribeReplicaLogDirsTurn::Progress => true,
        DescribeReplicaLogDirsTurn::Submit(submission) => {
            let (operation_id, deadline, broker_id, replicas, retained_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeReplicaLogDirsCall::submit(
                driver,
                broker_id,
                &replicas,
                retained_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, replicas, call)
                    .map_err(EngineHostError::DescribeReplicaLogDirs)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DescribeReplicaLogDirs)?,
            }
            true
        }
    };
    Ok(DescribeReplicaLogDirsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeReplicaLogDirsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeReplicaLogDirsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeReplicaLogDirsShardWakeError> {
        self.request()
            .map_err(|error| DescribeReplicaLogDirsShardWakeError::from_io(error.into_io()))
    }
}
