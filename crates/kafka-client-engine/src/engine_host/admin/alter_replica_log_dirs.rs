//! Fair host turns for exact-broker Admin `AlterReplicaLogDirs` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AlterReplicaLogDirsShardLockError, AlterReplicaLogDirsShardWake,
        AlterReplicaLogDirsShardWakeError, AlterReplicaLogDirsTurn,
    },
    driver::{AlterReplicaLogDirsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterReplicaLogDirsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterReplicaLogDirsProgress, EngineHostError> {
    let mut host = match resources.alter_replica_log_dirs.try_host() {
        Ok(host) => host,
        Err(AlterReplicaLogDirsShardLockError::Contended) => {
            return Ok(AlterReplicaLogDirsProgress::contended());
        }
        Err(AlterReplicaLogDirsShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterReplicaLogDirsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.alter_replica_log_dirs.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AlterReplicaLogDirs)?;
    let driver_progress = match turn {
        AlterReplicaLogDirsTurn::Idle => false,
        AlterReplicaLogDirsTurn::Progress => true,
        AlterReplicaLogDirsTurn::Submit(submission) => {
            let (
                operation_id,
                deadline,
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
            ) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AlterReplicaLogDirsCall::submit(
                driver,
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterReplicaLogDirs)?,
                Err(rejection) => {
                    let (broker_id, assignments, request_scratch_limit, result_limit) =
                        rejection.into_evidence();
                    host.reject_handoff(
                        operation_id,
                        broker_id,
                        assignments,
                        request_scratch_limit,
                        result_limit,
                    )
                    .map_err(EngineHostError::AlterReplicaLogDirs)?;
                }
            }
            true
        }
    };
    Ok(AlterReplicaLogDirsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterReplicaLogDirsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AlterReplicaLogDirsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterReplicaLogDirsShardWakeError> {
        self.request()
            .map_err(|error| AlterReplicaLogDirsShardWakeError::from_io(error.into_io()))
    }
}
