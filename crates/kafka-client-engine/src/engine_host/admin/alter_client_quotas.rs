//! Fair host turns for destructive `AnyBroker` Admin `AlterClientQuotas` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AlterClientQuotasShardLockError, AlterClientQuotasShardWake,
        AlterClientQuotasShardWakeError, AlterClientQuotasTurn,
    },
    driver::{AlterClientQuotasCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterClientQuotasProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterClientQuotasProgress, EngineHostError> {
    let mut host = match resources.alter_client_quotas.try_host() {
        Ok(host) => host,
        Err(AlterClientQuotasShardLockError::Contended) => {
            return Ok(AlterClientQuotasProgress::contended());
        }
        Err(AlterClientQuotasShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterClientQuotasLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.alter_client_quotas.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::AlterClientQuotas)?;
    let driver_progress = match turn {
        AlterClientQuotasTurn::Idle => false,
        AlterClientQuotasTurn::Progress => true,
        AlterClientQuotasTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_request_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match AlterClientQuotasCall::submit(
                driver,
                plan,
                retained_request_bytes,
                deadline.transport(),
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterClientQuotas)?,
                Err(rejection) => {
                    let (plan, retained_limit) = rejection.into_correlation();
                    host.reject_handoff(operation_id, plan, retained_limit)
                        .map_err(EngineHostError::AlterClientQuotas)?;
                }
            }
            true
        }
    };
    Ok(AlterClientQuotasProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterClientQuotasProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AlterClientQuotasShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterClientQuotasShardWakeError> {
        self.request()
            .map_err(|error| AlterClientQuotasShardWakeError::from_io(error.into_io()))
    }
}
