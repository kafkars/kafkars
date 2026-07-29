//! Bounded host turns for read-only share-group offset listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::list_share_group_offsets::{
        ListShareGroupOffsetsShardLockError, ListShareGroupOffsetsShardWake,
        ListShareGroupOffsetsShardWakeError, ListShareGroupOffsetsTurn,
    },
    driver::{ListShareGroupOffsetsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListShareGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListShareGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.list_share_group_offsets.try_host() {
        Ok(host) => host,
        Err(ListShareGroupOffsetsShardLockError::Contended) => {
            return Ok(ListShareGroupOffsetsProgress::contended());
        }
        Err(ListShareGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::ListShareGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.list_share_group_offsets.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ListShareGroupOffsets)?;
    let driver_progress = match turn {
        ListShareGroupOffsetsTurn::Idle => false,
        ListShareGroupOffsetsTurn::Progress => true,
        ListShareGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match ListShareGroupOffsetsCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListShareGroupOffsets)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::ListShareGroupOffsets)?,
            }
            true
        }
    };
    Ok(ListShareGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ListShareGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl ListShareGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListShareGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| ListShareGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}
