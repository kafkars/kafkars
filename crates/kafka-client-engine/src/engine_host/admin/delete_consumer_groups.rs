//! Fair host turns for coordinator-routed Admin `DeleteConsumerGroups` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{DeleteConsumerGroupsShardLockError, DeleteConsumerGroupsTurn},
    driver::DeleteConsumerGroupsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DeleteConsumerGroupsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteConsumerGroupsProgress, EngineHostError> {
    let mut host = match resources.delete_consumer_groups.try_host() {
        Ok(host) => host,
        Err(DeleteConsumerGroupsShardLockError::Contended) => {
            return Ok(DeleteConsumerGroupsProgress::contended());
        }
        Err(DeleteConsumerGroupsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteConsumerGroupsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.delete_consumer_groups.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DeleteConsumerGroups)?;
    let driver_progress = match turn {
        DeleteConsumerGroupsTurn::Idle => false,
        DeleteConsumerGroupsTurn::Progress => true,
        DeleteConsumerGroupsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, target, request_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DeleteConsumerGroupsCall::submit(
                driver,
                plan,
                target,
                request_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DeleteConsumerGroups)?,
                Err(rejection) => {
                    let (plan, target, request_limit, result_limit) =
                        rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, target, request_limit, result_limit)
                        .map_err(EngineHostError::DeleteConsumerGroups)?;
                }
            }
            true
        }
    };
    Ok(DeleteConsumerGroupsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteConsumerGroupsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
