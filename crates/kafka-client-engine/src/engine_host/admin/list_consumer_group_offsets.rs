//! Bounded host turns for concrete consumer-group offset listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{ListConsumerGroupOffsetsShardLockError, ListConsumerGroupOffsetsTurn},
    driver::GroupOffsetsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListConsumerGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListConsumerGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.list_consumer_group_offsets.try_host() {
        Ok(host) => host,
        Err(ListConsumerGroupOffsetsShardLockError::Contended) => {
            return Ok(ListConsumerGroupOffsetsProgress::contended());
        }
        Err(ListConsumerGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::ListConsumerGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .list_consumer_group_offsets
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ListConsumerGroupOffsets)?;
    let driver_progress = match turn {
        ListConsumerGroupOffsetsTurn::Idle => false,
        ListConsumerGroupOffsetsTurn::Progress => true,
        ListConsumerGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match GroupOffsetsCall::submit(
                driver,
                plan.group_id(),
                plan.require_stable(),
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListConsumerGroupOffsets)?,
                Err(rejection) => {
                    drop(rejection.into_source());
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::ListConsumerGroupOffsets)?;
                }
            }
            true
        }
    };
    Ok(ListConsumerGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ListConsumerGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
