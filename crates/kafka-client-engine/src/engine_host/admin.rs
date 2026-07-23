//! Bounded host turns for concrete controller-routed `CreateTopics` work.

use kafka_client_core::{CreateTopicsInput, Deadline, Moment};

use crate::admin::{CreateTopicsShardLockError, CreateTopicsTurn};

use super::{EngineHostError, EngineHostResources};

const CREATE_TOPICS_COMPLETION_BUDGET: usize = 16;

pub(super) struct AdminProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminProgress, EngineHostError> {
    let Some(permit) = resources.create_topics_calls.try_reserve() else {
        return snapshot(&resources.admin);
    };
    let mut host = match resources.admin.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => return Ok(AdminProgress::contended()),
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.admin.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::Admin)?;
    let driver_progress = match turn {
        CreateTopicsTurn::Idle => false,
        CreateTopicsTurn::Progress => true,
        CreateTopicsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, plan, retained_bytes, now) {
                Ok(()) => host
                    .apply(operation_id, CreateTopicsInput::DriverAccepted)
                    .map_err(EngineHostError::Admin)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.core_input())
                    .map_err(EngineHostError::Admin)?,
            }
            true
        }
    };
    Ok(AdminProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.admin.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => return Ok(false),
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..CREATE_TOPICS_COMPLETION_BUDGET {
        let Some(settled) = resources
            .create_topics_calls
            .poll_next_ready()
            .map_err(EngineHostError::CreateTopicsCompletion)?
        else {
            break;
        };
        let operation_id = settled.operation_id();
        let input = settled.take_input().ok_or(EngineHostError::Admin(
            crate::admin::CreateTopicsHostError::MissingTerminal,
        ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::Admin)?;
        resources.create_topics_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    admin: &crate::admin::CreateTopicsShardOwner,
) -> Result<AdminProgress, EngineHostError> {
    let host = match admin.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => return Ok(AdminProgress::contended()),
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminLockPoisoned);
        }
    };
    Ok(AdminProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl AdminProgress {
    pub(super) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
