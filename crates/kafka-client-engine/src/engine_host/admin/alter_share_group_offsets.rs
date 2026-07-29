//! Bounded host turns for destructive share-group offset alteration.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::alter_share_group_offsets::{
        AlterShareGroupOffsetsShardLockError, AlterShareGroupOffsetsShardWake,
        AlterShareGroupOffsetsShardWakeError, AlterShareGroupOffsetsTurn,
    },
    driver::{AlterShareGroupOffsetsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterShareGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterShareGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.alter_share_group_offsets.try_host() {
        Ok(host) => host,
        Err(AlterShareGroupOffsetsShardLockError::Contended) => {
            return Ok(AlterShareGroupOffsetsProgress::contended());
        }
        Err(AlterShareGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterShareGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.alter_share_group_offsets.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AlterShareGroupOffsets)?;
    let driver_progress = match turn {
        AlterShareGroupOffsetsTurn::Idle => false,
        AlterShareGroupOffsetsTurn::Progress => true,
        AlterShareGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AlterShareGroupOffsetsCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterShareGroupOffsets)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::AlterShareGroupOffsets)?,
            }
            true
        }
    };
    Ok(AlterShareGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterShareGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AlterShareGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterShareGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| AlterShareGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}
