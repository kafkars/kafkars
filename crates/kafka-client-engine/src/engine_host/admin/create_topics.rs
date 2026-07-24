//! Bounded host turns for concrete controller-routed `CreateTopics` work.

use kafka_client_core::{CreateTopicsInput, Deadline, Moment};

use crate::admin::{CreateTopicsShardLockError, CreateTopicsTurn};

use super::super::{EngineHostError, EngineHostResources};

const CREATE_TOPICS_COMPLETION_BUDGET: usize = 16;

pub(super) struct CreateTopicsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<CreateTopicsProgress, EngineHostError> {
    let Some(permit) = resources.create_topics_calls.try_reserve() else {
        return snapshot(&resources.create_topics);
    };
    let mut host = match resources.create_topics.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => {
            return Ok(CreateTopicsProgress::contended());
        }
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreateTopicsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.create_topics.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::CreateTopics)?;
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
                    .map_err(EngineHostError::CreateTopics)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.core_input())
                    .map_err(EngineHostError::CreateTopics)?,
            }
            true
        }
    };
    Ok(CreateTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.create_topics.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => return Ok(false),
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreateTopicsLockPoisoned);
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
        let input = settled.take_input().ok_or(EngineHostError::CreateTopics(
            crate::admin::CreateTopicsHostError::MissingTerminal,
        ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::CreateTopics)?;
        resources.create_topics_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::CreateTopicsShardOwner,
) -> Result<CreateTopicsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(CreateTopicsShardLockError::Contended) => {
            return Ok(CreateTopicsProgress::contended());
        }
        Err(CreateTopicsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreateTopicsLockPoisoned);
        }
    };
    Ok(CreateTopicsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl CreateTopicsProgress {
    pub(super) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
