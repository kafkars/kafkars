//! Bounded host turns for concrete consumer-group offset alteration.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AlterConsumerGroupOffsetsShardLockError, AlterConsumerGroupOffsetsTurn},
    driver::GroupOffsetAlterCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterConsumerGroupOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterConsumerGroupOffsetsProgress, EngineHostError> {
    let mut host = match resources.alter_consumer_group_offsets.try_host() {
        Ok(host) => host,
        Err(AlterConsumerGroupOffsetsShardLockError::Contended) => {
            return Ok(AlterConsumerGroupOffsetsProgress::contended());
        }
        Err(AlterConsumerGroupOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterConsumerGroupOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .alter_consumer_group_offsets
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AlterConsumerGroupOffsets)?;
    let driver_progress = match turn {
        AlterConsumerGroupOffsetsTurn::Idle => false,
        AlterConsumerGroupOffsetsTurn::Progress => true,
        AlterConsumerGroupOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, request_scratch_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match GroupOffsetAlterCall::submit(
                driver,
                plan,
                request_scratch_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterConsumerGroupOffsets)?,
                Err(rejection) => {
                    let (plan, request_scratch_limit, result_limit) =
                        rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, request_scratch_limit, result_limit)
                        .map_err(EngineHostError::AlterConsumerGroupOffsets)?;
                }
            }
            true
        }
    };
    Ok(AlterConsumerGroupOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterConsumerGroupOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
