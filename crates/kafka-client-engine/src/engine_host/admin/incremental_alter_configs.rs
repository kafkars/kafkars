//! Bounded host turns for concrete topic `IncrementalAlterConfigs` work.

use kafka_client_core::{Deadline, IncrementalAlterConfigsInput, Moment};

use crate::admin::{IncrementalAlterConfigsShardLockError, IncrementalAlterConfigsTurn};

use super::super::{EngineHostError, EngineHostResources};

const COMPLETION_BUDGET: usize = 16;

pub(super) struct IncrementalAlterConfigsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<IncrementalAlterConfigsProgress, EngineHostError> {
    let Some(permit) = resources.incremental_alter_configs_calls.try_reserve() else {
        return snapshot(&resources.incremental_alter_configs);
    };
    let mut host = match resources.incremental_alter_configs.try_host() {
        Ok(host) => host,
        Err(IncrementalAlterConfigsShardLockError::Contended) => {
            return Ok(IncrementalAlterConfigsProgress::contended());
        }
        Err(IncrementalAlterConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::IncrementalAlterConfigsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.incremental_alter_configs.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::IncrementalAlterConfigs)?;
    let driver_progress = match turn {
        IncrementalAlterConfigsTurn::Idle => false,
        IncrementalAlterConfigsTurn::Progress => true,
        IncrementalAlterConfigsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, plan, result_limit) {
                Ok(()) => host
                    .apply(operation_id, IncrementalAlterConfigsInput::DriverAccepted)
                    .map_err(EngineHostError::IncrementalAlterConfigs)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.into_core_input())
                    .map_err(EngineHostError::IncrementalAlterConfigs)?,
            }
            true
        }
    };
    Ok(IncrementalAlterConfigsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.incremental_alter_configs.try_host() {
        Ok(host) => host,
        Err(IncrementalAlterConfigsShardLockError::Contended) => return Ok(false),
        Err(IncrementalAlterConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::IncrementalAlterConfigsLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..COMPLETION_BUDGET {
        let Some(settled) = resources
            .incremental_alter_configs_calls
            .poll_next_ready()
            .map_err(EngineHostError::IncrementalAlterConfigsCompletion)?
        else {
            break;
        };
        let operation_id = settled.operation_id();
        let input = settled
            .take_input()
            .ok_or(EngineHostError::IncrementalAlterConfigs(
                crate::admin::IncrementalAlterConfigsHostError::MissingTerminal,
            ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::IncrementalAlterConfigs)?;
        resources.incremental_alter_configs_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::IncrementalAlterConfigsShardOwner,
) -> Result<IncrementalAlterConfigsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(IncrementalAlterConfigsShardLockError::Contended) => {
            return Ok(IncrementalAlterConfigsProgress::contended());
        }
        Err(IncrementalAlterConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::IncrementalAlterConfigsLockPoisoned);
        }
    };
    Ok(IncrementalAlterConfigsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl IncrementalAlterConfigsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
