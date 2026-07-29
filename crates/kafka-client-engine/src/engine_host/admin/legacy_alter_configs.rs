//! Bounded host turns for explicit legacy topic configuration replacement.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::legacy_alter_configs::{
        LegacyAlterConfigsShardLockError, LegacyAlterConfigsShardWake,
        LegacyAlterConfigsShardWakeError, LegacyAlterConfigsTurn,
    },
    driver::{LegacyAlterConfigsCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct LegacyAlterConfigsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<LegacyAlterConfigsProgress, EngineHostError> {
    let mut host = match resources.legacy_alter_configs.try_host() {
        Ok(host) => host,
        Err(LegacyAlterConfigsShardLockError::Contended) => {
            return Ok(LegacyAlterConfigsProgress::contended());
        }
        Err(LegacyAlterConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::LegacyAlterConfigsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.legacy_alter_configs.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::LegacyAlterConfigs)?;
    let driver_progress = match turn {
        LegacyAlterConfigsTurn::Idle => false,
        LegacyAlterConfigsTurn::Progress => true,
        LegacyAlterConfigsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match LegacyAlterConfigsCall::submit(driver, &plan, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::LegacyAlterConfigs)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::LegacyAlterConfigs)?,
            }
            true
        }
    };
    Ok(LegacyAlterConfigsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl LegacyAlterConfigsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl LegacyAlterConfigsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), LegacyAlterConfigsShardWakeError> {
        self.request()
            .map_err(|error| LegacyAlterConfigsShardWakeError::from_io(error.into_io()))
    }
}
