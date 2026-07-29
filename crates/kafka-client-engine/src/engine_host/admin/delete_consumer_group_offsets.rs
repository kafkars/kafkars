//! Bounded host turns for concrete consumer-group offset deletion.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{DeleteConsumerGroupOffsetsShardLockError, DeleteConsumerGroupOffsetsTurn},
    driver::GroupOffsetDeleteCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DeleteConsumerGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteConsumerGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.delete_consumer_group_offsets.try_host() {
        Ok(host) => host,
        Err(DeleteConsumerGroupOffsetsShardLockError::Contended) => {
            return Ok(DeleteConsumerGroupOffsetsProgress::contended());
        }
        Err(DeleteConsumerGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteConsumerGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .delete_consumer_group_offsets
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DeleteConsumerGroupOffsets)?;
    let driver_progress = match turn {
        DeleteConsumerGroupOffsetsTurn::Idle => false,
        DeleteConsumerGroupOffsetsTurn::Progress => true,
        DeleteConsumerGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, scratch_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match GroupOffsetDeleteCall::submit(driver, plan, scratch_limit, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DeleteConsumerGroupOffsets)?,
                Err(rejection) => {
                    let (plan, result_limit) = rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, result_limit)
                        .map_err(EngineHostError::DeleteConsumerGroupOffsets)?;
                }
            }
            true
        }
    };
    Ok(DeleteConsumerGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteConsumerGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
