//! Fair host turns for `AnyBroker` Admin `DeleteAcls` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        DeleteAclsShardLockError, DeleteAclsShardWake, DeleteAclsShardWakeError, DeleteAclsTurn,
    },
    driver::{DeleteAclsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DeleteAclsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteAclsProgress, EngineHostError> {
    let mut host = match resources.delete_acls.try_host() {
        Ok(host) => host,
        Err(DeleteAclsShardLockError::Contended) => {
            return Ok(DeleteAclsProgress::contended());
        }
        Err(DeleteAclsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteAclsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.delete_acls.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DeleteAcls)?;
    let driver_progress = match turn {
        DeleteAclsTurn::Idle => false,
        DeleteAclsTurn::Progress => true,
        DeleteAclsTurn::Submit(submission) => {
            let (
                operation_id,
                deadline,
                plan,
                request_limit,
                nested_count_capacity,
                result_capacity,
                outcome_capacity,
            ) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DeleteAclsCall::submit(
                driver,
                plan,
                request_limit,
                nested_count_capacity,
                result_capacity,
                outcome_capacity,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DeleteAcls)?,
                Err(rejection) => {
                    let (
                        plan,
                        request_limit,
                        nested_count_capacity,
                        result_capacity,
                        outcome_capacity,
                    ) = rejection.into_evidence();
                    host.reject_handoff(
                        operation_id,
                        plan,
                        request_limit,
                        nested_count_capacity,
                        result_capacity,
                        outcome_capacity,
                    )
                    .map_err(EngineHostError::DeleteAcls)?;
                }
            }
            true
        }
    };
    Ok(DeleteAclsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteAclsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DeleteAclsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteAclsShardWakeError> {
        self.request()
            .map_err(|error| DeleteAclsShardWakeError::from_io(error.into_io()))
    }
}
