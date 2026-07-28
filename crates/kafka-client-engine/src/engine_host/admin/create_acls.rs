//! Fair host turns for AnyBroker Admin `CreateAcls` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        CreateAclsShardLockError, CreateAclsShardWake, CreateAclsShardWakeError, CreateAclsTurn,
    },
    driver::{CreateAclsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct CreateAclsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<CreateAclsProgress, EngineHostError> {
    let mut host = match resources.create_acls.try_host() {
        Ok(host) => host,
        Err(CreateAclsShardLockError::Contended) => {
            return Ok(CreateAclsProgress::contended());
        }
        Err(CreateAclsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreateAclsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.create_acls.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::CreateAcls)?;
    let driver_progress = match turn {
        CreateAclsTurn::Idle => false,
        CreateAclsTurn::Progress => true,
        CreateAclsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, request_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match CreateAclsCall::submit(
                driver,
                plan,
                request_limit,
                result_limit,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::CreateAcls)?,
                Err(rejection) => {
                    let (plan, request_limit, result_limit) = rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, request_limit, result_limit)
                        .map_err(EngineHostError::CreateAcls)?;
                }
            }
            true
        }
    };
    Ok(CreateAclsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl CreateAclsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl CreateAclsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), CreateAclsShardWakeError> {
        self.request()
            .map_err(|error| CreateAclsShardWakeError::from_io(error.into_io()))
    }
}
