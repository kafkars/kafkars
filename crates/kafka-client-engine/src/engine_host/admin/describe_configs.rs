//! Bounded host turns for concrete topic `DescribeConfigs` work.

use kafka_client_core::{Deadline, DescribeConfigsInput, Moment};

use crate::admin::{DescribeConfigsShardLockError, DescribeConfigsTurn};

use super::super::{EngineHostError, EngineHostResources};

const DESCRIBE_CONFIGS_COMPLETION_BUDGET: usize = 16;

pub(super) struct DescribeConfigsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeConfigsProgress, EngineHostError> {
    let Some(permit) = resources.describe_configs_calls.try_reserve() else {
        return snapshot(&resources.describe_configs);
    };
    let mut host = match resources.describe_configs.try_host() {
        Ok(host) => host,
        Err(DescribeConfigsShardLockError::Contended) => {
            return Ok(DescribeConfigsProgress::contended());
        }
        Err(DescribeConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeConfigsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_configs.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeConfigs)?;
    let driver_progress = match turn {
        DescribeConfigsTurn::Idle => false,
        DescribeConfigsTurn::Progress => true,
        DescribeConfigsTurn::Submit(submission) => {
            let (operation_id, deadline, route, plan, result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, route, plan, result_limit) {
                Ok(()) => host
                    .apply(operation_id, DescribeConfigsInput::DriverAccepted)
                    .map_err(EngineHostError::DescribeConfigs)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.into_core_input())
                    .map_err(EngineHostError::DescribeConfigs)?,
            }
            true
        }
    };
    Ok(DescribeConfigsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.describe_configs.try_host() {
        Ok(host) => host,
        Err(DescribeConfigsShardLockError::Contended) => return Ok(false),
        Err(DescribeConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeConfigsLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..DESCRIBE_CONFIGS_COMPLETION_BUDGET {
        let Some(settled) = resources
            .describe_configs_calls
            .poll_next_ready()
            .map_err(EngineHostError::DescribeConfigsCompletion)?
        else {
            break;
        };
        let operation_id = settled.operation_id();
        let input = settled
            .take_input()
            .ok_or(EngineHostError::DescribeConfigs(
                crate::admin::DescribeConfigsHostError::MissingTerminal,
            ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::DescribeConfigs)?;
        resources.describe_configs_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::DescribeConfigsShardOwner,
) -> Result<DescribeConfigsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(DescribeConfigsShardLockError::Contended) => {
            return Ok(DescribeConfigsProgress::contended());
        }
        Err(DescribeConfigsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeConfigsLockPoisoned);
        }
    };
    Ok(DescribeConfigsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeConfigsProgress {
    pub(super) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
