//! Bounded host turns for destructive share-group offset deletion.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::delete_share_group_offsets::{
        DeleteShareGroupOffsetsShardLockError, DeleteShareGroupOffsetsShardWake,
        DeleteShareGroupOffsetsShardWakeError, DeleteShareGroupOffsetsTurn,
    },
    driver::{DeleteShareGroupOffsetsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DeleteShareGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteShareGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.delete_share_group_offsets.try_host() {
        Ok(host) => host,
        Err(DeleteShareGroupOffsetsShardLockError::Contended) => {
            return Ok(DeleteShareGroupOffsetsProgress::contended());
        }
        Err(DeleteShareGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteShareGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.delete_share_group_offsets.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DeleteShareGroupOffsets)?;
    let driver_progress = match turn {
        DeleteShareGroupOffsetsTurn::Idle => false,
        DeleteShareGroupOffsetsTurn::Progress => true,
        DeleteShareGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DeleteShareGroupOffsetsCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DeleteShareGroupOffsets)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DeleteShareGroupOffsets)?,
            }
            true
        }
    };
    Ok(DeleteShareGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteShareGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DeleteShareGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteShareGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| DeleteShareGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}
